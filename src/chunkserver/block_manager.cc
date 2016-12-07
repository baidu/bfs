// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//

#include "chunkserver/block_manager.h"

#include <sys/stat.h>
#include <sys/vfs.h>
#include <climits>
#include <functional>

#include <gflags/gflags.h>
#include <leveldb/db.h>
#include <leveldb/iterator.h>
#include <common/counter.h>
#include <common/logging.h>
#include <common/string_util.h>

#include "chunkserver/disk.h"
#include "chunkserver/data_block.h"
#include "chunkserver/file_cache.h"

DECLARE_int32(chunkserver_file_cache_size);
DECLARE_int32(chunkserver_use_root_partition);

namespace baidu {
namespace bfs {

extern common::Counter g_blocks;
extern common::Counter g_find_ops;

BlockManager::BlockManager(const std::string& store_path)
    : disk_quota_(0) {
    CheckStorePath(store_path);
    file_cache_ = new FileCache(FLAGS_chunkserver_file_cache_size);
}

BlockManager::~BlockManager() {
    MutexLock lock(&mu_);
    for (auto it = block_map_.begin(); it != block_map_.end(); ++it) {
        Block* block = it->second;
        if (!block->IsRecover()) {
            CloseBlock(block);
        } else {
            LOG(INFO, "[~BlockManager] Do not close recovering block #%ld ", block->Id());
        }
        block->DecRef();
    }
    for (size_t i = 0; i < disks_.size(); ++i) {
        delete disks_[i];
    }
    block_map_.clear();
    delete file_cache_;
    file_cache_ = NULL;
}

void BlockManager::CheckStorePath(const std::string& store_path) {
    int64_t disk_quota = 0;
    std::vector<std::string> store_path_list;
    common::SplitString(store_path, ",", &store_path_list);
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
    for (size_t i = 0; i < store_path_list.size(); ++i) {
        std::string& disk_path = store_path_list[i];
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
            Disk* disk = new Disk(store_path_list[i], file_cache_, user_quota);
            disks_.push_back(disk);
        } else {
            if (stat_ret != 0) {
                LOG(WARNING, "Stat store_path %s fail: %s, ignore it", disk_path.c_str(), strerror(errno));
            } else {
                LOG(WARNING, "%s's fsid is same to %s, ignore it",
                    disk_path.c_str(), fs_map[fsid_str].c_str());
            }
            store_path_list[i] = store_path_list[store_path_list.size() - 1];
            store_path_list.resize(store_path_list.size() - 1);
            --i;
        }
    }
    disk_quota_ = disk_quota;
}

int64_t BlockManager::DiskQuota() const {
    return disk_quota_;
}

// TODO: concurrent load
bool BlockManager::LoadStorage() {
    bool ret = true;
    for (size_t i = 0; i < disks_.size(); ++i) {
        ret = ret && disks_[i]->LoadStorage(std::bind(&BlockManager::AddBlock,
                                                      this, std::placeholders::_1,
                                                      std::placeholders::_2));
    }
    return ret;
}

int64_t BlockManager::NameSpaceVersion() const {
    int64_t version = -1;
    for (size_t i = 0; i < disks_.size(); ++i) {
        int64_t tmp = disks_[i]->NameSpaceVersion();
        if (version == -1) {
            version = tmp;
        }
        if (tmp != version) {
            return -1;
        }
    }
    return version;
}

bool BlockManager::SetNameSpaceVersion(int64_t version) {
    for (size_t i = 0; i < disks_.size(); ++i) {
        if (!disks_[i]->SetNameSpaceVersion(version)) {
            return false;
        }
    }
    return true;
}

// TODO: need test
int64_t BlockManager::ListBlocks(std::vector<BlockMeta>* blocks, int64_t offset, uint32_t num) {
    std::vector<leveldb::Iterator*> iters;
    for (size_t i = 0; i < disks_.size(); ++i) {
        disks_[i]->Seek(offset, &iters);
    }
    if (iters.size() == 0) {
        return 0;
    }
    int32_t idx = -1;
    int64_t largest_id = 0;
    while (blocks->size() < num && iters.size() != 0) {
        int64_t block_id = FindSmallest(iters, &idx);
        if (block_id == -1) {
            return largest_id;
        }
        auto it = iters[idx];
        BlockMeta meta;
        bool ret = meta.ParseFromArray(it->value().data(), it->value().size());
        assert(ret);
        assert(block_id == meta.block_id());
        blocks->push_back(meta);
        largest_id = block_id;
        iters[idx]->Next();
    }
    return largest_id;
}

Block* BlockManager::CreateBlock(int64_t block_id, StatusCode* status) {
    BlockMeta meta;
    meta.set_block_id(block_id);
    Disk* disk = PickDisk(block_id);
    meta.set_store_path(disk->Path());
    Block* block = new Block(meta, NULL, file_cache_);
    // for block_map_
    MutexLock lock(&mu_, "BlockManger::AddBlock", 1000);
    auto ret = block_map_.insert(std::make_pair(block_id, block));
    if (ret.second) {
        block->AddRef();
    } else {
        delete block;
        if (ret.first->second->IsFinished()) {
            *status = kReadOnly;
            return NULL;
        }
        *status = kBlockExist;
        return ret.first->second;
    }
    mu_.Unlock();
    if (!disk->SyncBlockMeta(meta)) {
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
    LOG(INFO, "CreateBlock %ld on %s", block_id, disk->Path().c_str());
    return block;
}

bool BlockManager::CloseBlock(Block* block) {
    return block->Close();
}

StatusCode BlockManager::RemoveBlock(int64_t block_id) {
    Block* block = FindBlock(block_id);
    if (!block) {
        LOG(INFO, "Try to remove block that does not exist: #%ld ", block_id);
        return kNsNotFound;
    } else {
        MutexLock lock(&mu_, "BlockManager::RemoveBlock erase", 1000);
        block_map_.erase(block_id);
        block->DecRef();
        LOG(INFO, "Remove #%ld meta info done, ref= %ld", block_id, block->GetRef());
    }
    StatusCode s = block->SetDeleted();
    if (s != kOK) {
        LOG(INFO, "Block #%ld deleted failed, %s", block_id, StatusCode_Name(s).c_str());
        block->DecRef();
        return s;
    }
    // disk file will be removed in block's deconstrucor
    file_cache_->EraseFileCache(block->GetFilePath());
    block->DecRef();
    return kOK;
}

// TODO: concurrent & async cleanup
bool BlockManager::CleanUp(int64_t namespace_version) {
    for (size_t i = 0; i < disks_.size(); ++i) {
        if (disks_[i]->NameSpaceVersion() != namespace_version) {
            if (!disks_[i]->CleanUp()) {
                return false;
            }
        }
    }
    LOG(INFO, "CleanUp done");
    return true;
}

bool BlockManager::AddBlock(int64_t block_id, Block* block) {
    MutexLock lock(&mu_);
    return block_map_.insert(std::make_pair(block_id, block)).second;
}

Block* BlockManager::FindBlock(int64_t block_id) {
    g_find_ops.Inc();
    MutexLock lock(&mu_, "BlockManger::Find", 1000);
    auto it = block_map_.find(block_id);
    if (it == block_map_.end()) {
        // not found
        return NULL;
    }
    Block* block = it->second;
    // for user
    block->AddRef();
    return block;
}

Disk* BlockManager::PickDisk(int64_t block_id) {
    return disks_[block_id % disks_.size()];
}

int64_t BlockManager::FindSmallest(std::vector<leveldb::Iterator*>& iters, int32_t* idx) {
    int64_t id = LLONG_MAX;
    for (size_t i = 0; i < iters.size(); ++i) {
        auto it = iters[i];
        if (!it->Valid()) {
            delete it;
            iters[i] = iters[iters.size() - 1];
            iters.resize(iters.size() - 1);
            --i;
            continue;
        }
        int64_t tmp;
        if (1 != sscanf(it->key().data(), "%ld", &tmp)) {
            LOG(WARNING, "[FindSmallest] Unknown meta key: %s\n",
                it->key().ToString().c_str());
            return -1;
        }
        if (tmp < id) {
            id = tmp;
            *idx = i;
        }
    }
    assert(id != LLONG_MAX);
    return id;
}

} // namespace bfs
} // namespace baidu
