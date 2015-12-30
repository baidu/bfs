// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#include "chunkserver/block_manager.h"

#include <errno.h>
#include <string.h>
#include <sys/vfs.h>
#include <boost/bind.hpp>

#include <gflags/gflags.h>
#include <leveldb/db.h>
#include <leveldb/cache.h>
#include <common/counter.h>
#include <common/logging.h>
#include <common/string_util.h>

#include "chunkserver/data_block.h"
#include "chunkserver/file_cache.h"

DECLARE_int32(chunkserver_file_cache_size);

namespace baidu {
namespace bfs {

extern common::Counter g_data_size;
extern common::Counter g_find_ops;

BlockManager::BlockManager(ThreadPool* thread_pool, const std::string& store_path)
    :_thread_pool(thread_pool),
     _metadb(NULL),
     _namespace_version(0), _disk_quota(0) {
     CheckStorePath(store_path);
     _file_cache = new FileCache(FLAGS_chunkserver_file_cache_size);
}
BlockManager::~BlockManager() {
    for (BlockMap::iterator it = _block_map.begin();
            it != _block_map.end(); ++it) {
        it->second->DecRef();
    }
    _block_map.clear();
    delete _metadb;
    _metadb = NULL;
}
int64_t BlockManager::DiskQuota() const{
    return _disk_quota;
}
void BlockManager::CheckStorePath(const std::string& store_path) {
    int64_t disk_quota = 0;
    common::SplitString(store_path, ",", &_store_path_list);
    for (uint32_t i = 0; i < _store_path_list.size(); ++i) {
       std::string& disk_path = _store_path_list[i];
       disk_path = common::TrimString(disk_path, " ");
       if (disk_path.empty() || disk_path[disk_path.size() - 1] != '/') {
           disk_path += "/";
       }
       struct statfs fs_info;
       if (0 == statfs(disk_path.c_str(), &fs_info)) {
           int64_t disk_size = fs_info.f_blocks * fs_info.f_bsize;
           int64_t user_quota = fs_info.f_bavail * fs_info.f_bsize;
           int64_t super_quota = fs_info.f_bfree * fs_info.f_bsize;
           LOG(INFO, "Use store path: %s block: %ld disk: %s available %s quota: %s",
               disk_path.c_str(), fs_info.f_bsize,
               common::HumanReadableString(disk_size).c_str(),
               common::HumanReadableString(super_quota).c_str(),
               common::HumanReadableString(user_quota).c_str());
           disk_quota += user_quota;
       } else {
           LOG(WARNING, "Stat store_path %s fail, ignore it", disk_path.c_str());
           _store_path_list[i] = _store_path_list[_store_path_list.size() - 1];
           _store_path_list.resize(_store_path_list.size() - 1);
           --i;
       }
    }
    std::sort(_store_path_list.begin(), _store_path_list.end());
    std::vector<std::string>::iterator it
       = std::unique(_store_path_list.begin(), _store_path_list.end());
    _store_path_list.resize(std::distance(_store_path_list.begin(), it));
    LOG(INFO, "%lu store path used.", _store_path_list.size());
    assert(_store_path_list.size() > 0);
    _disk_quota = disk_quota;
}
const std::string& BlockManager::GetStorePath(int64_t block_id) {
    return _store_path_list[block_id % _store_path_list.size()];
}
/// Load meta from disk
bool BlockManager::LoadStorage() {
    MutexLock lock(&_mu);
    leveldb::Options options;
    options.create_if_missing = true;
    leveldb::Status s = leveldb::DB::Open(options, _store_path_list[0] + "meta/", &_metadb);
    if (!s.ok()) {
        LOG(WARNING, "Load blocks fail: %s", s.ToString().c_str());
        return false;
    }

    std::string version_key(8, '\0');
    version_key.append("version");
    std::string version_str;
    s = _metadb->Get(leveldb::ReadOptions(), version_key, &version_str);
    if (s.ok() && version_str.size() == 8) {
        _namespace_version = *(reinterpret_cast<int64_t*>(&version_str[0]));
        LOG(INFO, "Load namespace %ld", _namespace_version);
    }
    int block_num = 0;
    leveldb::Iterator* it = _metadb->NewIterator(leveldb::ReadOptions());
    for (it->Seek(version_key+'\0'); it->Valid(); it->Next()) {
        int64_t block_id = 0;
        if (1 != sscanf(it->key().data(), "%ld", &block_id)) {
            LOG(WARNING, "Unknown key: %s\n", it->key().ToString().c_str());
            delete it;
            return false;
        }
        BlockMeta meta;
        assert(it->value().size() == sizeof(meta));
        memcpy(&meta, it->value().data(), sizeof(meta));
        assert(meta.block_id == block_id);
        Block* block = new Block(meta, GetStorePath(block_id), _thread_pool, _file_cache);
        block->AddRef();
        _block_map[block_id] = block;
        block_num ++;
    }
    delete it;
    LOG(INFO, "Load %ld blocks, namespace version: %ld", block_num, _namespace_version);
    if (_namespace_version == 0 && block_num > 0) {
        LOG(WARNING, "Namespace version lost!");
    }
    _disk_quota += g_data_size.Get();
    return true;
}
int64_t BlockManager::NameSpaceVersion() const {
    return _namespace_version;
}
bool BlockManager::SetNameSpaceVersion(int64_t version) {
    MutexLock lock(&_mu);
    std::string version_key(8, '\0');
    version_key.append("version");
    std::string version_str(8, '\0');
    *(reinterpret_cast<int64_t*>(&version_str[0])) = version;
    leveldb::Status s = _metadb->Put(leveldb::WriteOptions(), version_key, version_str);
    if (!s.ok()) {
        LOG(WARNING, "SetNameSpaceVersion fail: %s", s.ToString().c_str());
        return false;
    }
    _namespace_version = version;
    LOG(INFO, "Set namespace version: %ld", _namespace_version);
    return true;
}
bool BlockManager::ListBlocks(std::vector<BlockMeta>* blocks, int64_t offset, int32_t num) {
    leveldb::Iterator* it = _metadb->NewIterator(leveldb::ReadOptions());
    for (it->Seek(BlockId2Str(offset)); it->Valid(); it->Next()) {
        int64_t block_id = 0;
        if (1 != sscanf(it->key().data(), "%ld", &block_id)) {
            LOG(WARNING, "[ListBlocks] Unknown meta key: %s\n",
                it->key().ToString().c_str());
            delete it;
            return false;
        }
        BlockMeta meta;
        assert(it->value().size() == sizeof(meta));
        memcpy(&meta, it->value().data(), sizeof(meta));
        assert(meta.block_id == block_id);
        blocks->push_back(meta);
        // LOG(DEBUG, "List block %ld", block_id);
        if (--num <= 0) {
            break;
        }
    }
    delete it;
    return true;
}

Block* BlockManager::FindBlock(int64_t block_id, bool create_if_missing, int64_t* sync_time) {
    Block* block = NULL;
    {
        MutexLock lock(&_mu, "BlockManger::Find", 1000);
        g_find_ops.Inc();
        BlockMap::iterator it = _block_map.find(block_id);
        if (it != _block_map.end()) {
            block = it->second;
        } else if (create_if_missing) {
            BlockMeta meta;
            meta.block_id = block_id;
            meta.version = 0;
            block = new Block(meta, GetStorePath(block_id), _thread_pool, _file_cache);
            // for block_map
            block->AddRef();
            _block_map[block_id] = block;
            // Unlock for write meta & sync
            _mu.Unlock();
            if (!SyncBlockMeta(meta, sync_time)) {
                delete block;
                block = NULL;
            }
            _mu.Lock();
            if (!block) {
                _block_map.erase(block_id);
            }
        } else {
            // not found
        }
    }
    // for user
    if (block) {
        block->AddRef();
    }
    return block;
}
std::string BlockManager::BlockId2Str(int64_t block_id) {
    char idstr[64];
    snprintf(idstr, sizeof(idstr), "%13ld", block_id);
    return std::string(idstr);
}
bool BlockManager::SyncBlockMeta(const BlockMeta& meta, int64_t* sync_time) {
    std::string idstr = BlockId2Str(meta.block_id);
    leveldb::WriteOptions options;
    // options.sync = true;
    int64_t time_start = common::timer::get_micros();
    leveldb::Status s = _metadb->Put(options, idstr,
        leveldb::Slice(reinterpret_cast<const char*>(&meta),sizeof(meta)));
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
bool BlockManager::RemoveBlock(int64_t block_id) {
    Block* block = NULL;
    {
        MutexLock lock(&_mu, "BlockManager::RemoveBlock", 1000);
        BlockMap::iterator it = _block_map.find(block_id);
        if (it == _block_map.end()) {
            LOG(INFO, "Try to remove block that does not exist: #%ld ", block_id);
            return false;
        }
        block = it->second;
        if (!block->SetDeleted()) {
            LOG(INFO, "Block #%ld deleted by other thread", block_id);
            return false;
        }
        block->AddRef();
    }

    int64_t du = block->DiskUsed();
    std::string file_path = block->GetFilePath();
    _file_cache->EraseFileCache(file_path);
    int ret = remove(file_path.c_str());
    if (ret != 0 && (errno !=2 || du > 0)) {
        LOG(WARNING, "Remove #%ld disk file %s %ld bytes fails: %d (%s)",
            block_id, file_path.c_str(), du, errno, strerror(errno));
    } else {
        LOG(INFO, "Remove #%ld disk file done: %s\n",
            block_id, file_path.c_str());
    }

    char dir_name[5];
    snprintf(dir_name, sizeof(dir_name), "/%03ld", block_id % 1000);
    // Rmdir, ignore error when not empty.
    // rmdir((GetStorePath(block_id) + dir_name).c_str());
    char idstr[14];
    snprintf(idstr, sizeof(idstr), "%13ld", block_id);

    leveldb::Status s = _metadb->Delete(leveldb::WriteOptions(), idstr);
    if (s.ok()) {
        LOG(INFO, "Remove #%ld meta info done", block_id);
        {
            MutexLock lock(&_mu, "BlockManager::RemoveBlock erase", 1000);
            _block_map.erase(block_id);
        }
        block->DecRef();
        ret = true;
    } else {
        LOG(WARNING, "Remove #%ld meta info fails: %s", block_id, s.ToString().c_str());
        ret = false;
    }
    block->DecRef();
    return ret;
}

}
}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
