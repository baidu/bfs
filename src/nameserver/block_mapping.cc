// Copyright (c) 2014, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "block_mapping.h"

#include <boost/bind.hpp>
#include <gflags/gflags.h>

#include <common/counter.h>
#include <common/logging.h>

DECLARE_int32(default_replica_num);

namespace baidu {
namespace bfs {

common::Counter g_recovering_blocks_hi;
common::Counter g_recovering_blocks_lo;
common::Counter g_recovering_blocks_ck;

NSBlock::NSBlock(int64_t block_id)
 : id(block_id), version(-1), block_size(0),
   expect_replica_num(FLAGS_default_replica_num) {
    g_recovering_blocks_hi.Clear();
    g_recovering_blocks_lo.Clear();
    g_recovering_blocks_ck.Clear();
}

BlockMapping::BlockMapping() : next_block_id_(1) {}

int64_t BlockMapping::NewBlockID() {
    MutexLock lock(&mu_, "BlockMapping::NewBlockID", 1000);
    return next_block_id_++;
}

bool BlockMapping::GetBlock(int64_t block_id, NSBlock* block) {
    MutexLock lock(&mu_, "BlockMapping::GetBlock", 1000);
    NSBlockMap::iterator it = block_map_.find(block_id);
    if (it == block_map_.end()) {
        return false;
    }
    if (block) {
        *block = *(it->second);
    }
    return true;
}

bool BlockMapping::GetReplicaLocation(int64_t id, std::set<int32_t>* chunkserver_id) {
    MutexLock lock(&mu_);
    NSBlock* nsblock = NULL;
    NSBlockMap::iterator it = block_map_.find(id);
    bool ret = false;
    if (it != block_map_.end()) {
        nsblock = it->second;
        *chunkserver_id = nsblock->replica;
        ret = true;
    } else {
        LOG(WARNING, "Can't find block: #%ld ", id);
    }

    return ret;
}

bool BlockMapping::ChangeReplicaNum(int64_t block_id, int32_t replica_num) {
    MutexLock lock(&mu_);
    NSBlockMap::iterator it = block_map_.find(block_id);
    if (it == block_map_.end()) {
        assert(0);
    } else {
        NSBlock* nsblock = it->second;
        nsblock->expect_replica_num = replica_num;
        return true;
    }
}

void BlockMapping::AddNewBlock(int64_t block_id) {
    MutexLock lock(&mu_);
    NSBlock* nsblock = NULL;
    NSBlockMap::iterator it = block_map_.find(block_id);
    //Don't suppport soft link now
    assert(it == block_map_.end());
    nsblock = new NSBlock(block_id);
    block_map_[block_id] = nsblock;
    LOG(DEBUG, "Init block info: #%ld ", block_id);
    if (next_block_id_ <= block_id) {
        next_block_id_ = block_id + 1;
    }
}

bool BlockMapping::UpdateBlockInfo(int64_t id, int32_t server_id, int64_t block_size,
                     int64_t block_version) {
    MutexLock lock(&mu_);
    NSBlock* nsblock = NULL;
    NSBlockMap::iterator it = block_map_.find(id);
    if (it == block_map_.end()) {
        //have been removed
        LOG(DEBUG, "UpdateBlockInfo(%ld) has been removed", id);
        return false;
    } else {
        nsblock = it->second;
        if (nsblock->version >= 0 && block_version >= 0 &&
                nsblock->version != block_version) {
            LOG(INFO, "block #%ld on slow chunkserver: %d,"
                    " NSB version: %ld, cs version: %ld, drop it",
                    id, server_id, nsblock->version, block_version);
            return false;
        }
        if (nsblock->block_size !=  block_size && block_size) {
            // update
            if (nsblock->block_size) {
                LOG(WARNING, "block #%ld size mismatch", id);
                assert(0);
                return false;
            } else {
                LOG(INFO, "block #%ld size update, %ld to %ld",
                    id, nsblock->block_size, block_size);
                nsblock->block_size = block_size;
            }
        } else {
            //LOG(DEBUG, "UpdateBlockInfo(%ld) ignored, from %ld to %ld",
            //    id, nsblock->block_size, block_size);
        }
    }
    std::pair<std::set<int32_t>::iterator, bool> ret = nsblock->replica.insert(server_id);
    int32_t cur_replica_num = nsblock->replica.size();
    int32_t expect_replica_num = nsblock->expect_replica_num;
    if (cur_replica_num > expect_replica_num) {
        LOG(INFO, "too much replica cur=%d expect=%d server=%d",
            server_id, cur_replica_num, expect_replica_num);
        nsblock->replica.erase(ret.first);
        return false;
    } else {
        LOG(INFO, "Need to recover for #%ld, cur=%d expect=%d", id, cur_replica_num, expect_replica_num);
        AddToRecover(nsblock, true);
    }
    return true;
}

void BlockMapping::RemoveBlocksForFile(const FileInfo& file_info) {
    for (int i = 0; i < file_info.blocks_size(); i++) {
        int64_t block_id = file_info.blocks(i);
        std::set<int32_t> chunkservers;
        GetReplicaLocation(block_id, &chunkservers);
        RemoveBlock(block_id);
        LOG(INFO, "Remove block #%ld for %s", block_id, file_info.name().c_str());
    }
}

void BlockMapping::RemoveBlock(int64_t block_id) {
    MutexLock lock(&mu_);
    NSBlockMap::iterator it = block_map_.find(block_id);
    if (it == block_map_.end()) {
        LOG(WARNING, "RemoveBlock(%ld) not found", block_id);
        return;
    }
    delete it->second;
    block_map_.erase(it);
}

bool BlockMapping::SetBlockVersion(int64_t block_id, int64_t version) {
    bool ret = true;
    MutexLock lock(&mu_);
    NSBlockMap::iterator it = block_map_.find(block_id);
    if (it == block_map_.end()) {
        LOG(WARNING, "Can't find block: #%ld ", block_id);
        ret = false;
    } else {
        it->second->version = version;
    }
    return ret;
}

void BlockMapping::PickRecoverBlocks(int64_t cs_id, int64_t block_num,
                                     std::map<int64_t, int64_t>* recover_blocks) {
    MutexLock lock(&mu_);
    int n = 0;
    std::set<int64_t> tmp_blocks;
    for (RecoverList::iterator it = recover_hi_.begin(); n < block_num && it != recover_hi_.end(); ++it) {
        NSBlock* cur_block = it->second;
        const std::set<int32_t>& replica = cur_block->replica;
        if (!NeedRecover(cur_block)) {
            recover_hi_.erase(it++);
            continue;
        }
        if (replica.find(cs_id) != replica.end()) {
            continue;
            ++it;
        } else {
            (*recover_blocks)[it->first] = *replica.begin();
            recover_check_.insert(std::make_pair<int64_t, int64_t>(it->first, cs_id));
            tmp_blocks.insert(it->first);
            recover_hi_.erase(it++);
            ++n;
        }
    }
    for (RecoverList::iterator it = recover_lo_.begin(); n < block_num && it != recover_lo_.end(); ++it) {
        NSBlock* cur_block = it->second;
        const std::set<int32_t>& replica = cur_block->replica;
        if (!NeedRecover(cur_block)) {
            recover_lo_.erase(it++);
            continue;
        }
        if (replica.find(cs_id) != replica.end() || tmp_blocks.find(it->first) != tmp_blocks.end()) {
            continue;
            ++it;
        } else {
            (*recover_blocks)[it->first] = *replica.begin();
            recover_check_.insert(std::make_pair<int64_t, int64_t>(it->first, cs_id));
            recover_lo_.erase(it++);
            ++n;
        }
    }
}

void BlockMapping::AddToRecover(NSBlock* nsblock, bool need_check) {
    mu_.AssertHeld();
    int64_t block_id = nsblock->id;
    if (!need_check ||
        recover_check_.find(block_id) != recover_check_.end() ||
        recover_hi_.find(block_id) != recover_hi_.end() ||
        recover_lo_.find(block_id) != recover_lo_.end()) {
        // block is being recovering
        return;
    }
    if (nsblock->replica.size() == 0) {
        LOG(WARNING, "Lost block #%ld", block_id);
    } else if (nsblock->replica.size() == 1) {
        recover_hi_[block_id] = nsblock;
        recover_lo_.erase(block_id);
    } else {
        recover_lo_[block_id] = nsblock;
        recover_hi_.erase(block_id);
    }
}

bool BlockMapping::NeedRecover(NSBlock* nsblock) {
    if (!nsblock) {
        LOG(INFO, "Can't find block: #%ld ", nsblock->id);
        return false;
    }
    if (nsblock->replica.size() == 0) {
        LOG(WARNING, "Lost block #%ld", nsblock->id);
        return false;
    }
    if (nsblock->replica.size() == nsblock->expect_replica_num) {
        return false;
    }
    return true;
}

void BlockMapping::CheckRecover(int64_t block_id, int64_t cs_id) {
    NSBlockMap::iterator mapping_it = block_map_.find(block_id);
    if (mapping_it == block_map_.end()) {
        LOG(INFO, "Can't find block: #%ld ", block_id);
        reutrn;
    }

    std::pair<CheckList::iterator, CheckList::iterator> ret = recover_check_.equal_range(block_id);
    CheckList::iterator block_it = ret.first;
    for (; block_it != ret.second; ++block_it) {
        if (block_it->second == cs_id) {
            break;
        }
    }
    if (block_it == ret.second) {
        LOG(INFO, "Can't find recovered block: block_id=%ld cs_id=%ld ", block_id, cs_id);
        return;
    }
    if (mapping_it->second.replica.size() != mapping_it->second.expect_replica_num) {
        AddToRecover(mapping_it->second, false);
    }
}

} // namespace bfs
} // namespace baidu
