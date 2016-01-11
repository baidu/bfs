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

common::Counter g_recovering;
common::Counter g_recovering_blocks_ck;

NSBlock::NSBlock(int64_t block_id)
 : id(block_id), version(-1), block_size(0),
   expect_replica_num(FLAGS_default_replica_num), pending_recover(false) {
    g_recovering.Clear();
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
                     int64_t block_version, bool need_recovery) {
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
    } else if (cur_replica_num < expect_replica_num) {
        if (need_recovery) {
            LOG(INFO, "Need to recover for #%ld, cur=%d expect=%d", id, cur_replica_num, expect_replica_num);
            AddToRecover(nsblock);
        }
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

void BlockMapping::DealWithDeadBlocks(int64_t cs_id, std::set<int64_t> blocks) {
    MutexLock lock(&mu_);
    for (std::set<int64_t>::iterator it = blocks.begin(); it != blocks.end(); ++it) {
        NSBlockMap::iterator b_it = block_map_.find(*it);
        if (b_it == block_map_.end()) {
            LOG(INFO, "Can't find block: #%ld ", *it);
            continue;
        }
        b_it->second->replica.erase(cs_id);
        AddToRecover(b_it->second);
    }
}

void BlockMapping::PickRecoverBlocks(int64_t cs_id, int64_t block_num,
                                     std::map<int64_t, int64_t>* recover_blocks) {
    MutexLock lock(&mu_);
    std::set<NSBlock*> tmp_holder;
    int64_t n = 0;
    while (n < block_num && !recover_q_.empty()) {
        NSBlock* cur_block = recover_q_.top();
        if (!cur_block) { // block is removed
            LOG(DEBUG, "Can't find block: #%ld ", cur_block->id);
            recover_q_.pop();
            continue;
        }
        if (cur_block->replica.size() == cur_block->expect_replica_num) {
            LOG(DEBUG, "enough replica #%ld", cur_block->id);
            cur_block->pending_recover = false;
            recover_q_.pop();
            continue;
        }
        if (cur_block->replica.find(cs_id) != cur_block->replica.end()) {
            tmp_holder.insert(cur_block);
            recover_q_.pop();
            continue;
        }
        recover_blocks->insert(std::make_pair<int64_t, int64_t>
                                    (cur_block->id, *(cur_block->replica.begin())));
        recover_check_[cur_block->id] = cur_block;
        thread_pool_.DelayTask(5000, boost::bind(&BlockMapping::CheckRecover, this, cur_block->id));
        recover_q_.pop();
        ++n;
    }
    for (std::set<NSBlock*>::iterator it = tmp_holder.begin(); it != tmp_holder.end(); ++it) {
        recover_q_.push(*it);
    }
}

void BlockMapping::ProcessRecoveredBlock(int64_t cs_id, int64_t block_id) {
    MutexLock lock(&mu_);
    CheckList::iterator check_it = recover_check_.find(block_id);
    if (check_it == recover_check_.end()) {
        LOG(DEBUG, "not in recover_check_ #%ld cs_id=", block_id, cs_id);
    }
    NSBlock* b = check_it->second;
    b->replica.insert(cs_id);
    b->pending_recover = false;
}

void BlockMapping::GetStat(int64_t* recover_num, int64_t* pending_num) {
    MutexLock lock(&mu_);
    if (recover_num) {
        *recover_num = recover_q_.size();
    }
    if (pending_num) {
        *pending_num = recover_check_.size();
    }
}

void BlockMapping::AddToRecover(NSBlock* block) {
    mu_.AssertHeld();
    if (!block->pending_recover) {
        recover_q_.push(block);
        block->pending_recover = true;
        LOG(DEBUG, "add to recover block_id=%ld replica=%ld", block->id, block->replica.size());
    } else {
        LOG(DEBUG, "block pending, ignore. block_id=%ld", block->id);
    }
}

void BlockMapping::CheckRecover(int64_t block_id) {
    MutexLock lock(&mu_);
    CheckList::iterator check_it = recover_check_.find(block_id);
    assert(check_it != recover_check_.end());
    NSBlock* block = check_it->second;
    if (!block) {
        LOG(DEBUG, "Can't find block: #%ld ", block_id);
        return;
    }
    if (block->replica.size() < block->expect_replica_num) {
        recover_q_.push(block);
    } else {
        block->pending_recover = false;
    }
    recover_check_.erase(block_id);
}

} // namespace bfs
} // namespace baidu
