// Copyright (c) 2014, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "block_mapping.h"

#include <vector>
#include <boost/bind.hpp>
#include <gflags/gflags.h>

#include <common/logging.h>

DECLARE_int32(default_replica_num);
DECLARE_int32(recover_speed);
DECLARE_int32(recover_timeout);

namespace baidu {
namespace bfs {

NSBlock::NSBlock(int64_t block_id)
 : id(block_id), version(-1), block_size(0),
   expect_replica_num(FLAGS_default_replica_num), pending_recover(false) {}

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
            AddToRecover(nsblock);
        }
    }
    return true;
}

void BlockMapping::RemoveBlocksForFile(const FileInfo& file_info) {
    for (int i = 0; i < file_info.blocks_size(); i++) {
        int64_t block_id = file_info.blocks(i);
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

void BlockMapping::DealWithDeadBlocks(int64_t cs_id, const std::set<int64_t>& blocks) {
    MutexLock lock(&mu_);
    NSBlock* block = NULL;
    for (std::set<int64_t>::iterator it = blocks.begin(); it != blocks.end(); ++it) {
        if (!GetBlockPtr(*it, &block)) {
            LOG(DEBUG, "DealWithDeadBlocks for %d can't find block: #%ld ", cs_id, *it);
            continue;
        }
        block->replica.erase(cs_id);
        AddToRecover(block);
    }
}

int32_t BlockMapping::PickRecoverBlocks(int32_t cs_id, int32_t block_num,
                                     std::map<int64_t, int32_t>* recover_blocks) {
    MutexLock lock(&mu_);
    std::vector<std::pair<int64_t, int64_t> > tmp_holder;
    CheckList::iterator check_it = recover_check_.insert(std::make_pair(cs_id, std::set<int64_t>())).first;
    int32_t quota = FLAGS_recover_speed - (check_it->second).size();
    LOG(DEBUG, "cs %d has %d pending_recover blocks", cs_id, (check_it->second).size());
    quota = quota < block_num ? quota : block_num;
    int32_t n = 0;
    while (n < quota && !recover_q_.empty()) {
        std::pair<int64_t, int64_t> recover_item = recover_q_.top();
        NSBlock* cur_block = NULL;
        if (!GetBlockPtr(recover_item.second, &cur_block)) { // block is removed
            LOG(DEBUG, "PickRecoverBlocks for %d can't find block: #%ld ",
                cs_id, recover_item.second);
            recover_q_.pop();
            continue;
        }
        if (static_cast<int64_t>(cur_block->replica.size()) == cur_block->expect_replica_num) {
            cur_block->pending_recover = false;
            recover_q_.pop();
            continue;
        }
        if (cur_block->replica.find(cs_id) != cur_block->replica.end()) {
            tmp_holder.push_back(recover_item);
            recover_q_.pop();
            continue;
        }
        recover_blocks->insert(std::make_pair(cur_block->id, *(cur_block->replica.begin())));
        (check_it->second).insert(cur_block->id);
        thread_pool_.DelayTask(FLAGS_recover_timeout * 1000,
            boost::bind(&BlockMapping::CheckRecover, this, cs_id, cur_block->id));
        recover_q_.pop();
        ++n;
    }
    for (std::vector<std::pair<int64_t, int64_t> >::iterator it = tmp_holder.begin();
         it != tmp_holder.end(); ++it) {
        recover_q_.push(*it);
    }
    LOG(DEBUG, "recover_q_ size = %d", recover_q_.size());
    return n;
}

void BlockMapping::ProcessRecoveredBlock(int64_t cs_id, int64_t block_id, bool recover_success) {
    MutexLock lock(&mu_);
    NSBlock* b = NULL;
    if (!GetBlockPtr(block_id, &b)) {
        LOG(DEBUG, "ProcessRecoveredBlock for %d can't find block: #%ld ", cs_id, block_id);
        return;
    }
    if (recover_success) {
        b->replica.insert(cs_id);
        LOG(DEBUG, "recovered  block #%ld at %ld", block_id, cs_id);
    }
    CheckList::iterator it = recover_check_.find(cs_id);
    if (it == recover_check_.end()) {
        LOG(DEBUG, "not in recover_check_ #%ld cs_id %ld", block_id, cs_id);
        return;
    }
    (it->second).erase(block_id);
    TryRecover(b);
}

void BlockMapping::GetStat(int64_t* recover_num, int64_t* pending_num) {
    MutexLock lock(&mu_);
    if (recover_num) {
        *recover_num = recover_q_.size();
    }
    if (pending_num) {
        *pending_num = 0;
        for (CheckList::iterator it = recover_check_.begin(); it != recover_check_.end(); ++it) {
            *pending_num += (it->second).size();
        }
    }
}

void BlockMapping::AddToRecover(NSBlock* block) {
    mu_.AssertHeld();
    if (!block->pending_recover) {
        recover_q_.push(std::make_pair(block->expect_replica_num - block->replica.size(), block->id));
        block->pending_recover = true;
        LOG(DEBUG, "add to recover block_id #%ld replica=%ld", block->id, block->replica.size());
    }
}

void BlockMapping::TryRecover(NSBlock* block) {
    mu_.AssertHeld();
    int64_t replica_diff = block->expect_replica_num - block->replica.size();
    if (replica_diff > 0) {
        recover_q_.push(std::make_pair(replica_diff, block->id));
        LOG(DEBUG, "need more recover: #%ld pending %d", block->id, block->pending_recover);
    } else {
        block->pending_recover = false;
        LOG(DEBUG, "recover done: #%ld ", block->id);
    }
}

void BlockMapping::CheckRecover(int64_t cs_id, int64_t block_id) {
    MutexLock lock(&mu_);
    LOG(DEBUG, "recover timeout check: #%ld ", block_id);
    CheckList::iterator it = recover_check_.find(cs_id);
    if (it == recover_check_.end()) return;
    std::set<int64_t>& block_set = it->second;
    std::set<int64_t>::iterator check_it = block_set.find(block_id);
    if (check_it == block_set.end()) {
        return;
    }
    NSBlock* block = NULL;
    if (!GetBlockPtr(block_id, &block)) {
        LOG(DEBUG, "CheckRecover for %d can't find block: #%ld ", cs_id, block_id);
        block_set.erase(block_id);
        return;
    }
    block_set.erase(block_id);
    TryRecover(block);
}

bool BlockMapping::GetBlockPtr(int64_t block_id, NSBlock** block) {
    mu_.AssertHeld();
    NSBlockMap::iterator it = block_map_.find(block_id);
    if (it == block_map_.end()) {
        return false;
    }
    if (block) {
        *block = it->second;
    }
    return true;
}

} // namespace bfs
} // namespace baidu
