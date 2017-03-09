// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//

#include "chunkserver/block_manager.h"

#include <sys/stat.h>
#include <sys/vfs.h>
#include <climits>
#include <functional>
#include <algorithm>

#include <gflags/gflags.h>
#include <leveldb/db.h>
#include <leveldb/iterator.h>
#include <common/counter.h>
#include <common/logging.h>
#include <common/string_util.h>

#include "chunkserver/disk.h"
#include "chunkserver/data_block.h"
#include "chunkserver/file_cache.h"
#include "utils/meta_converter.h"

DECLARE_int32(chunkserver_file_cache_size);
DECLARE_int32(chunkserver_use_root_partition);
DECLARE_bool(chunkserver_multi_path_on_one_disk);
DECLARE_int32(chunkserver_disk_buf_size);

namespace baidu {
namespace bfs {

extern common::Counter g_find_ops;

BlockManager::BlockManager(const std::string& store_path)
    : thread_pool_(new ThreadPool(1)), disk_quota_(0), counter_manager_(new DiskCounterManager) {
    CheckStorePath(store_path);
    file_cache_ = new FileCache(FLAGS_chunkserver_file_cache_size);
    LogStatus();
}

BlockManager::~BlockManager() {
    thread_pool_->Stop(true);
    delete thread_pool_;
    delete counter_manager_;
    MutexLock lock(&mu_);
    for (auto it = block_map_.begin(); it != block_map_.end(); ++it) {
        Block* block = it->second;
        if (!block->IsRecover()) {
            CloseBlock(block, true);
        } else {
            LOG(INFO, "[~BlockManager] Do not close recovering block #%ld ", block->Id());
        }
        block->DecRef();
    }
    for (auto it = disks_.begin(); it != disks_.end(); ++it) {
        delete it->second;
    }
    block_map_.clear();
    delete file_cache_;
    file_cache_ = NULL;
}

DiskStat BlockManager::Stat() {
    MutexLock lock(&mu_);
    return stat_;
}

void BlockManager::Stat(std::string* str) {
    str->append("<table class=dataintable>");
    str->append("<tr><td>Path</td><td>Blocks</td><td>Quota</td><td>Size</td>"
                "<td>BufWrite</td><td>DiskWrite</td><td>Read m/d</td>"
                "<td>PenBuf</td><td>WritingBlocks</td></tr>");
    for (auto it = disks_.begin(); it != disks_.end(); ++it) {
        const DiskStat& stat = it->first;
        int64_t quota = it->second->GetQuota();
        int64_t size = stat.data_size;
        double ratio = size * 100.0 / quota;
        std::string ratio_str = common::NumToString(ratio);
        str->append("<tr><td>" + it->second->Path() + "</td>");
        str->append("<td>" + common::NumToString(stat.blocks) + "</td>");
        str->append("<td>" + common::HumanReadableString(quota) + "</td>");
        str->append("<td>" + common::HumanReadableString(size) + "</td>");
        str->append("<td>" + common::HumanReadableString(stat.buf_write_bytes) + "</td>");
        str->append("<td>" + common::HumanReadableString(stat.disk_write_bytes) + "</td>");
        str->append("<td>" + common::NumToString(stat.mem_read_ops) +"/" +
                    common::NumToString(stat.disk_read_ops) + "</td>");
        str->append("<td>" + common::NumToString(stat.pending_buf));
        str->append("<td>" + common::NumToString(stat.writing_blocks));
    }
    str->append("</table>");
}

void BlockManager::CheckStorePath(const std::string& store_path) {
    std::string fsid_str;
    struct statfs fs_info;
    std::string home_fs;
    if (statfs("/home", &fs_info) == 0) {
        home_fs.assign((const char*)&fs_info.f_fsid, sizeof(fs_info.f_fsid));
    } else if (statfs("/", &fs_info) == 0) {
        LOG(WARNING, "statfs(\"/home\") fail: %s", strerror(errno));
        home_fs.assign((const char*)&fs_info.f_fsid, sizeof(fs_info.f_fsid));
    } else {
        LOG(FATAL, "statfs(\"/\") fail: %s", strerror(errno));
    }

    std::vector<std::string> store_path_list;
    common::SplitString(store_path, ",", &store_path_list);
    for (uint32_t i = 0; i < store_path_list.size(); ++i) {
        std::string& disk_path = store_path_list[i];
        disk_path = common::TrimString(disk_path, " ");
        if (disk_path.empty() || disk_path[disk_path.size() - 1] != '/') {
            disk_path += "/";
        }
    }
    std::sort(store_path_list.begin(), store_path_list.end());
    auto it = std::unique(store_path_list.begin(), store_path_list.end());
    store_path_list.resize(std::distance(store_path_list.begin(), it));

    std::set<std::string> fsids;
    int64_t disk_quota = 0;
    for (uint32_t i = 0; i < store_path_list.size(); ++i) {
        std::string& disk_path = store_path_list[i];
        int stat_ret = statfs(disk_path.c_str(), &fs_info);
        std::string fs_tmp((const char*)&fs_info.f_fsid, sizeof(fs_info.f_fsid));
        if (stat_ret != 0 ||
            (!FLAGS_chunkserver_multi_path_on_one_disk && fsids.find(fs_tmp) != fsids.end()) ||
            (!FLAGS_chunkserver_use_root_partition && fs_tmp == home_fs)) {
            // statfs failed
            // do not allow multi data path on the same disk
            // do not allow using root as data path
            if (stat_ret != 0) {
                LOG(WARNING, "Stat store_path %s fail: %s, ignore it",
                        disk_path.c_str(), strerror(errno));
            } else {
                LOG(WARNING, "%s fsid has been used", disk_path.c_str());
            }
            store_path_list[i] = store_path_list[store_path_list.size() - 1];
            store_path_list.resize(store_path_list.size() - 1);
            --i;
        } else {
            int64_t disk_size = fs_info.f_blocks * fs_info.f_bsize;
            int64_t user_quota = fs_info.f_bavail * fs_info.f_bsize;
            int64_t super_quota = fs_info.f_bfree * fs_info.f_bsize;
            LOG(INFO, "Use store path: %s block: %ld disk: %s available %s quota: %s",
                    disk_path.c_str(), fs_info.f_bsize,
                    common::HumanReadableString(disk_size).c_str(),
                    common::HumanReadableString(super_quota).c_str(),
                    common::HumanReadableString(user_quota).c_str());
            disk_quota += user_quota;
            Disk* disk = new Disk(store_path_list[i], user_quota);
            disks_.push_back(std::make_pair(DiskStat(), disk));
            fsids.insert(fs_tmp);
        }
    }
    LOG(INFO, "%lu store path used.", store_path_list.size());
    assert(store_path_list.size() > 0);
    CheckChunkserverMeta(store_path_list);
}

int64_t BlockManager::DiskQuota() const {
    return disk_quota_;
}

// TODO: concurrent load
bool BlockManager::LoadStorage() {
    bool ret = true;
    for (auto it = disks_.begin(); it != disks_.end(); ++it) {
        Disk* disk = it->second;
        ret = ret && disk->LoadStorage(std::bind(&BlockManager::AddBlock,
                                                      this, std::placeholders::_1,
                                                      std::placeholders::_2,
                                                      std::placeholders::_3));
        disk_quota_ += disk->GetQuota();
    }
    return ret;
}

int64_t BlockManager::NamespaceVersion() const {
    int64_t version = -1;
    for (auto it = disks_.begin(); it != disks_.end(); ++it) {
        Disk* disk = it->second;
        int64_t tmp = disk->NamespaceVersion();
        if (version == -1) {
            version = tmp;
        }
        if (tmp != version) {
            return -1;
        }
    }
    return version;
}

bool BlockManager::SetNamespaceVersion(int64_t version) {
    for (auto it = disks_.begin(); it != disks_.end(); ++it) {
        Disk* disk = it->second;
        if (!disk->SetNamespaceVersion(version)) {
            return false;
        }
    }
    return true;
}

int64_t BlockManager::ListBlocks(std::vector<BlockMeta>* blocks, int64_t offset, uint32_t num) {
    std::vector<leveldb::Iterator*> iters;
    for (auto it = disks_.begin(); it != disks_.end(); ++it) {
        Disk* disk = it->second;
        disk->Seek(offset, &iters);
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
    for (size_t i = 0; i < iters.size(); ++i) {
        delete iters[i];
    }
    return largest_id;
}

Block* BlockManager::CreateBlock(int64_t block_id, StatusCode* status) {
    BlockMeta meta;
    meta.set_block_id(block_id);
    Disk* disk = PickDisk(block_id);
    if (!disk) {
        *status = kNotEnoughQuota;
        LOG(WARNING, "Not enough space on disk");
        return NULL;
    }
    meta.set_store_path(disk->Path());
    Block* block = new Block(meta, disk, file_cache_);
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
    LOG(INFO, "CreateBlock #%ld on %s", block_id, disk->Path().c_str());
    return block;
}

bool BlockManager::CloseBlock(Block* block, bool sync) {
    return block->Close(sync);
}

StatusCode BlockManager::RemoveBlock(int64_t block_id) {
    Block* block = FindBlock(block_id);
    if (!block) {
        LOG(INFO, "Try to remove block that does not exist: #%ld ", block_id);
        return kCsNotFound;
    } else {
        MutexLock lock(&mu_, "BlockManager::RemoveBlock erase", 1000);
        if (block_map_.erase(block_id)) {
            block->DecRef();
            LOG(INFO, "Remove #%ld meta info done, ref= %ld", block_id, block->GetRef());
        } else {
            LOG(INFO, "#%ld has alreadly been erased from map, ref= %ld", block_id, block->GetRef());
        }
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
    for (auto it = block_map_.begin(); it != block_map_.end();) {
        Block* block = it->second;
        if (block->CleanUp(namespace_version)) {
            file_cache_->EraseFileCache(block->GetFilePath());
            block->DecRef();
            block_map_.erase(it++);
        } else {
            ++it;
        }
    }
    LOG(INFO, "CleanUp done");
    return true;
}

bool BlockManager::AddBlock(int64_t block_id, Disk* disk, BlockMeta meta) {
    Block* block = new Block(meta, disk, file_cache_);
    block->AddRef();
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
    double min_load = kDiskMaxLoad - 1;
    Disk* target = NULL;
    for (auto it = disks_.begin(); it != disks_.end(); ++it) {
        Disk* disk = it->second;
        double load = disk->GetLoad();
        if (load < min_load) {
            min_load = load;
            target = disk;
        }
    }
    return target;
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
            delete it;
            iters[i] = iters[iters.size() - 1];
            iters.resize(iters.size() - 1);
            --i;
            continue;
        }
        if (tmp < id) {
            id = tmp;
            *idx = i;
        }
    }
    if (id == LLONG_MAX) {
        if (iters.size() != 0) {
            LOG(WARNING, "[FindSmallest] error iters is not empty");
        }
        return -1;
    }
    return id;
}

void BlockManager::LogStatus() {
    DiskStat stat_tmp;
    for (auto it = disks_.begin(); it != disks_.end(); ++it) {
        Disk* disk = it->second;
        DiskStat stat = disk->Stat();
        it->first = stat;

        stat_tmp.blocks += stat.blocks;
        stat_tmp.buf_write_bytes += stat.buf_write_bytes;
        stat_tmp.disk_write_bytes += stat.disk_write_bytes;
        stat_tmp.writing_blocks += stat.writing_blocks;
        stat_tmp.writing_bytes += stat.writing_bytes;
        stat_tmp.data_size += stat.data_size;
        stat_tmp.pending_buf += stat.pending_buf;
        stat_tmp.mem_read_ops += stat.mem_read_ops;
        stat_tmp.disk_read_ops += stat.disk_read_ops;
    }
    mu_.Lock();
    stat_ = stat_tmp;
    mu_.Unlock();
    std::string str;
    stat_.ToString(&str);
    LOG(INFO, "[DiskStat] %s", str.c_str());
    thread_pool_->DelayTask(1000, std::bind(&BlockManager::LogStatus, this));
}

} // namespace bfs
} // namespace baidu
