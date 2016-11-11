// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#include "chunkserver/block_manager.h"

#include <errno.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/vfs.h>
#include <functional>
#include <algorithm>

#include <gflags/gflags.h>
#include <leveldb/db.h>
#include <leveldb/cache.h>
#include <common/counter.h>
#include <common/logging.h>
#include <common/string_util.h>

#include "chunkserver/data_block.h"
#include "chunkserver/file_cache.h"

DECLARE_int32(chunkserver_file_cache_size);
DECLARE_int32(chunkserver_use_root_partition);
DECLARE_int32(chunkserver_io_thread_num);

namespace baidu {
namespace bfs {

extern common::Counter g_blocks;
extern common::Counter g_data_size;
extern common::Counter g_find_ops;

BlockManager::BlockManager(const std::string& store_path)
   : metadb_(NULL),
     namespace_version_(0), disk_quota_(0) {
     CheckStorePath(store_path);
     thread_pool_ = new ThreadPool(FLAGS_chunkserver_io_thread_num);
     file_cache_ = new FileCache(FLAGS_chunkserver_file_cache_size);
}
BlockManager::~BlockManager() {
    MutexLock lock(&mu_);
    for (BlockMap::iterator it = block_map_.begin(); it != block_map_.end(); ++it) {
        Block* block = it->second;
        if (!block->IsRecover()) {
            CloseBlock(block);
        } else {
            LOG(INFO, "[~BlockManager] Do not close recovering block #%ld ", block->Id());
        }
        block->DecRef();
    }
    thread_pool_->Stop(true);
    delete thread_pool_;
    block_map_.clear();
    delete metadb_;
    metadb_ = NULL;
    delete file_cache_;
    file_cache_ = NULL;
}
int64_t BlockManager::DiskQuota() const{
    return disk_quota_;
}

void BlockManager::CheckStorePath(const std::string& store_path) {
    int64_t disk_quota = 0;
    common::SplitString(store_path, ",", &store_path_list_);
    std::map<std::string, std::string> fs_map;
    std::string fsid_str;
    struct statfs fs_info;
    int stat_ret = statfs("/home", &fs_info);
    if (stat_ret != 0 && statfs("/", &fs_info) != 0) {
        LOG(FATAL, "statfs(\"/\") fail: %s", strerror(errno));
    } else if (FLAGS_chunkserver_use_root_partition == 0) {
        fsid_str.assign((const char*)&fs_info.f_fsid, sizeof(fs_info.f_fsid));
        fs_map[fsid_str] = "Root";
        LOG(INFO, "Root fsid: %s", common::DebugString(fsid_str).c_str());
    }
    for (uint32_t i = 0; i < store_path_list_.size(); ++i) {
        std::string& disk_path = store_path_list_[i];
        disk_path = common::TrimString(disk_path, " ");
        if (disk_path.empty() || disk_path[disk_path.size() - 1] != '/') {
            disk_path += "/";
        }
        if (0 == (stat_ret = statfs(disk_path.c_str(), &fs_info))
          && (fsid_str.assign((const char*)&fs_info.f_fsid, sizeof(fs_info.f_fsid)),
              fs_map.find(fsid_str) == fs_map.end())) {
            int64_t disk_size = fs_info.f_blocks * fs_info.f_bsize;
            int64_t user_quota = fs_info.f_bavail * fs_info.f_bsize;
            int64_t super_quota = fs_info.f_bfree * fs_info.f_bsize;
            fs_map[fsid_str] = disk_path;
            LOG(INFO, "Use store path: %s block: %ld disk: %s available %s quota: %s",
                disk_path.c_str(), fs_info.f_bsize,
                common::HumanReadableString(disk_size).c_str(),
                common::HumanReadableString(super_quota).c_str(),
                common::HumanReadableString(user_quota).c_str());
            disk_quota += user_quota;
        } else {
            if (stat_ret != 0) {
                LOG(WARNING, "Stat store_path %s fail: %s, ignore it", disk_path.c_str(), strerror(errno));
            } else {
                LOG(WARNING, "%s's fsid is same to %s, ignore it",
                    disk_path.c_str(), fs_map[fsid_str].c_str());
            }
            store_path_list_[i] = store_path_list_[store_path_list_.size() - 1];
            store_path_list_.resize(store_path_list_.size() - 1);
            --i;
        }
    }
    std::sort(store_path_list_.begin(), store_path_list_.end());
    std::vector<std::string>::iterator it
       = std::unique(store_path_list_.begin(), store_path_list_.end());
    store_path_list_.resize(std::distance(store_path_list_.begin(), it));
    LOG(INFO, "%lu store path used.", store_path_list_.size());
    assert(store_path_list_.size() > 0);
    disk_quota_ = disk_quota;
}
const std::string& BlockManager::GetStorePath(int64_t block_id) {
    return store_path_list_[block_id % store_path_list_.size()];
}
/// Load meta from disk
bool BlockManager::LoadStorage() {
    MutexLock lock(&mu_);
    int64_t start_load_time = common::timer::get_micros();
    leveldb::Options options;
    options.create_if_missing = true;
    leveldb::Status s = leveldb::DB::Open(options, store_path_list_[0] + "meta/", &metadb_);
    if (!s.ok()) {
        LOG(WARNING, "Load blocks fail: %s", s.ToString().c_str());
        return false;
    }

    std::string version_key(8, '\0');
    version_key.append("version");
    std::string version_str;
    s = metadb_->Get(leveldb::ReadOptions(), version_key, &version_str);
    if (s.ok() && version_str.size() == 8) {
        namespace_version_ = *(reinterpret_cast<int64_t*>(&version_str[0]));
        LOG(INFO, "Load namespace %ld", namespace_version_);
    }
    int block_num = 0;
    leveldb::Iterator* it = metadb_->NewIterator(leveldb::ReadOptions());
    for (it->Seek(version_key+'\0'); it->Valid(); it->Next()) {
        int64_t block_id = 0;
        if (1 != sscanf(it->key().data(), "%ld", &block_id)) {
            LOG(WARNING, "Unknown key: %s\n", it->key().ToString().c_str());
            delete it;
            return false;
        }
        BlockMeta meta;
        if (!meta.ParseFromArray(it->value().data(), it->value().size())
            || meta.block_id() != block_id) {
            struct OldBlockMeta {
                int64_t block_id;
                int64_t block_size;
                int64_t checksum;
                int64_t version;
            };
            assert(it->value().size() == sizeof(struct OldBlockMeta));
            OldBlockMeta oldmeta;
            memcpy(&oldmeta, it->value().data(), it->value().size());
            assert(oldmeta.block_id == block_id);
            meta.set_block_id(oldmeta.block_id);
            meta.set_block_size(oldmeta.block_size);
            meta.set_checksum(oldmeta.checksum);
            meta.set_version(oldmeta.version);
            meta.set_store_path(GetStorePath(block_id));
            std::string meta_buf;
            meta.SerializeToString(&meta_buf);
            metadb_->Put(leveldb::WriteOptions(), it->key(), meta_buf);
            LOG(INFO, "Old meta info of #%ld has been trans to new meta info", oldmeta.block_id);
        }
        std::string file_path = meta.store_path() + Block::BuildFilePath(block_id);
        if (meta.version() < 0) {
            LOG(INFO, "Incomplete block #%ld V%ld %ld, drop it",
                block_id, meta.version(), meta.block_size());
            metadb_->Delete(leveldb::WriteOptions(), it->key());
            remove(file_path.c_str());
            continue;
        } else {
            if (std::find(store_path_list_.begin(), store_path_list_.end(), meta.store_path())
                    == store_path_list_.end()) {
                LOG(WARNING, "Block #%ld store path %s not in current store configuration, ignore it",
                    block_id, file_path.c_str());
                continue;
            }
            struct stat st;
            if (stat(file_path.c_str(), &st) ||
                st.st_size != meta.block_size() ||
                access(file_path.c_str(), R_OK)) {
                LOG(WARNING, "Corrupted block #%ld V%ld size %ld path %s can't access: %s'",
                    block_id, meta.version(), meta.block_size(), file_path.c_str(),
                    strerror(errno));
                metadb_->Delete(leveldb::WriteOptions(), it->key());
                remove(file_path.c_str());
                continue;
            } else {
                LOG(DEBUG, "Load #%ld V%ld size %ld path %s",
                    block_id, meta.version(), meta.block_size(), file_path.c_str());
            }
        }
        Block* block = new Block(meta, thread_pool_, file_cache_);
        block->AddRef();
        block_map_[block_id] = block;
        block_num ++;
    }
    delete it;
    int64_t end_load_time = common::timer::get_micros();
    LOG(INFO, "Load %ld blocks, use %ld ms, namespace version: %ld",
        block_num, (end_load_time - start_load_time) / 1000, namespace_version_);
    if (namespace_version_ == 0 && block_num > 0) {
        LOG(WARNING, "Namespace version lost!");
    }
    disk_quota_ += g_data_size.Get();
    return true;
}
int64_t BlockManager::NameSpaceVersion() const {
    return namespace_version_;
}
bool BlockManager::SetNameSpaceVersion(int64_t version) {
    MutexLock lock(&mu_);
    std::string version_key(8, '\0');
    version_key.append("version");
    std::string version_str(8, '\0');
    *(reinterpret_cast<int64_t*>(&version_str[0])) = version;
    leveldb::Status s = metadb_->Put(leveldb::WriteOptions(), version_key, version_str);
    if (!s.ok()) {
        LOG(WARNING, "SetNameSpaceVersion fail: %s", s.ToString().c_str());
        return false;
    }
    namespace_version_ = version;
    LOG(INFO, "Set namespace version: %ld", namespace_version_);
    return true;
}

int64_t BlockManager::ListBlocks(std::vector<BlockMeta>* blocks, int64_t offset, int32_t num) {
    leveldb::Iterator* it = metadb_->NewIterator(leveldb::ReadOptions());
    int64_t largest_id = 0;
    for (it->Seek(BlockId2Str(offset)); it->Valid(); it->Next()) {
        int64_t block_id = 0;
        if (1 != sscanf(it->key().data(), "%ld", &block_id)) {
            LOG(WARNING, "[ListBlocks] Unknown meta key: %s\n",
                it->key().ToString().c_str());
            break;
        }
        BlockMeta meta;
        bool ret = meta.ParseFromArray(it->value().data(), it->value().size());
        assert(ret);
        //skip blocks not in current configuration
        if (find(store_path_list_.begin(), store_path_list_.end(), meta.store_path())
                  == store_path_list_.end()) {
            continue;
        }
        assert(meta.block_id() == block_id);
        blocks->push_back(meta);
        largest_id = block_id;
        // LOG(DEBUG, "List block %ld", block_id);
        if (--num <= 0) {
            break;
        }
    }
    delete it;
    return largest_id;
}

Block* BlockManager::CreateBlock(int64_t block_id, int64_t* sync_time, StatusCode* status) {
    BlockMeta meta;
    meta.set_block_id(block_id);
    meta.set_store_path(GetStorePath(block_id));
    Block* block = new Block(meta, thread_pool_, file_cache_);
    MutexLock lock(&mu_, "BlockManger::AddBlock", 1000);
    BlockMap::iterator it = block_map_.find(block_id);
    if (it != block_map_.end()) {
        delete block;
        if (it->second->IsFinished()) {
            *status = kReadOnly;
            return NULL;
        }
        *status = kBlockExist;
        return it->second;
    }
    // for block_map
    block->AddRef();
    block_map_[block_id] = block;
    // Unlock for write meta & sync
    mu_.Unlock();
    if (!SyncBlockMeta(meta, sync_time)) {
        delete block;
        *status = kSyncMetaFailed;
        block = NULL;
    }
    mu_.Lock();
    if (!block) {
        block_map_.erase(block_id);
    } else {
        // for user
        block->AddRef();
    }
    *status = kOK;
    return block;
}

Block* BlockManager::FindBlock(int64_t block_id) {
    g_find_ops.Inc();
    MutexLock lock(&mu_, "BlockManger::Find", 1000);
    BlockMap::iterator it = block_map_.find(block_id);
    if (it == block_map_.end()) {
        // not found
        return NULL;
    }
    Block* block = it->second;
    // for user
    block->AddRef();
    return block;
}
std::string BlockManager::BlockId2Str(int64_t block_id) {
    char idstr[64];
    snprintf(idstr, sizeof(idstr), "%13ld", block_id);
    return std::string(idstr);
}
bool BlockManager::SyncBlockMeta(const BlockMeta& meta, int64_t* sync_time) {
    std::string idstr = BlockId2Str(meta.block_id());
    leveldb::WriteOptions options;
    // options.sync = true;
    int64_t time_start = common::timer::get_micros();
    std::string meta_buf;
    meta.SerializeToString(&meta_buf);
    leveldb::Status s = metadb_->Put(options, idstr, meta_buf);
    int64_t time_use = common::timer::get_micros() - time_start;
    if (sync_time) *sync_time = time_use;
    if (!s.ok()) {
        Log(WARNING, "Write to meta fail:%s", idstr.c_str());
        return false;
    }
    return true;
}
bool BlockManager::CloseBlock(Block* block) {
    if (!block->Close()) {
        return false;
    }

    // Update meta
    BlockMeta meta = block->GetMeta();
    return SyncBlockMeta(meta, NULL);
}

bool BlockManager::RemoveBlockMeta(int64_t block_id) {
    char idstr[14];
    snprintf(idstr, sizeof(idstr), "%13ld", block_id);
    leveldb::Status s = metadb_->Delete(leveldb::WriteOptions(), idstr);
    if (!s.ok()) {
        LOG(WARNING, "Remove #%ld meta info fails: %s", block_id, s.ToString().c_str());
        return false;
    }
    return true;
}
bool BlockManager::RemoveBlock(int64_t block_id) {
    bool meta_removed = RemoveBlockMeta(block_id);
    Block* block = FindBlock(block_id);
    if (block == NULL) {
        LOG(INFO, "Try to remove block that does not exist: #%ld ", block_id);
        return false;
    }
    if (!block->SetDeleted()) {
        LOG(INFO, "Block #%ld deleted by other thread", block_id);
        block->DecRef();
        return false;
    }

    // disk file will be removed in block's deconstrucor
    file_cache_->EraseFileCache(block->GetFilePath());

    bool ret = false;
    if (meta_removed) {
        MutexLock lock(&mu_, "BlockManager::RemoveBlock erase", 1000);
        block_map_.erase(block_id);
        block->DecRef();
        LOG(INFO, "Remove #%ld meta info done, ref= %ld", block_id, block->GetRef());
        ret = true;
    }
    block->DecRef();
    return ret;
}

bool BlockManager::RemoveAllBlocks() {
    LOG(INFO, "RemoveAllBlocks...");
    if (!RemoveAllBlocksAsync()) {
        return false;
    }
    int count = 0;
    while (g_blocks.Get() > 0) {
        if (count++ %1000 == 0) {
            LOG(INFO, "RemoveAllBlocks wait done now block num: %ld", g_blocks.Get());
        }
        usleep(10000);
    }
    LOG(INFO, "RemoveAllBlocks done");
    return true;
}

bool BlockManager::RemoveAllBlocksAsync() {
    leveldb::Iterator* it = metadb_->NewIterator(leveldb::ReadOptions());
    for (it->Seek(BlockId2Str(0)); it->Valid(); it->Next()) {
        int64_t block_id = 0;
        if (1 != sscanf(it->key().data(), "%ld", &block_id)) {
            LOG(FATAL, "[ListBlocks] Unknown meta key: %s\n",
                it->key().ToString().c_str());
            delete it;
            return false;
        }
        thread_pool_->AddTask(std::bind(&BlockManager::RemoveBlock, this, block_id));
    }
    delete it;
    return true;
}

}
}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
