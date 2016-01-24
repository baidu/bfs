// Copyright (c) 2014, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "block_mapping.h"

#include <vector>
#include <boost/bind.hpp>
#include <gflags/gflags.h>

#include <common/logging.h>

DECLARE_int32(recover_speed);
DECLARE_int32(recover_timeout);

namespace baidu {
namespace bfs {

NSBlock::NSBlock()
    : id(-1), version(-1), block_size(-1),
      expect_replica_num(0), pending_recover(false), incomplete(false) {
}
NSBlock::NSBlock(int64_t block_id, int32_t replica,
                 int64_t block_version, int64_t block_size)
    : id(block_id), version(block_version), block_size(block_size),
      expect_replica_num(replica), pending_recover(false), incomplete(false) {
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

void BlockMapping::AddNewBlock(int64_t block_id, int32_t replica,
                               int64_t version, int64_t size,
                               const std::vector<int32_t>* init_replicas) {
    NSBlock* nsblock = NULL;
    nsblock = new NSBlock(block_id, replica, version, size);
    if (init_replicas) {
        for (uint32_t i = 0; i < init_replicas->size(); i++) {
            nsblock->replica.insert(init_replicas->at(i));
        }
    }
    if (init_replicas) {
        LOG(DEBUG, "Init block info: #%ld ", block_id);
    } else {
        LOG(DEBUG, "Rebuild block info: #%ld ", block_id);
    }

    MutexLock lock(&mu_);
    std::pair<NSBlockMap::iterator, bool> ret =
        block_map_.insert(std::make_pair(block_id,nsblock));
    assert(ret.second == true);
    if (next_block_id_ <= block_id) {
        next_block_id_ = block_id + 1;
    }
}

bool BlockMapping::UpdateBlockInfo(int64_t id, int32_t server_id, int64_t block_size,
                     int64_t block_version, bool safe_mode) {
    MutexLock lock(&mu_);
    NSBlock* nsblock = NULL;
    NSBlockMap::iterator it = block_map_.find(id);
    if (it == block_map_.end()) {
        //have been removed
        LOG(DEBUG, "UpdateBlockInfo #%ld has been removed", id);
        return false;
    }
    nsblock = it->second;
    if (nsblock->version >= 0) {
        if (block_version >= 0 && nsblock->version != block_version) {
            LOG(INFO, "block #%ld on slow chunkserver: %d,"
                    " Ns: V%ld cs: V%ld drop it",
                    id, server_id, nsblock->version, block_version);
            nsblock->replica.erase(server_id);
            return false;
        } else if (block_version < 0) {
            /// pulling block?
            return true;
        } else {
            assert(block_version >= 0 && block_version == nsblock->version);
            /// another received block
        }
    }
    if (nsblock->block_size !=  block_size && block_size) {
        // update
        if (nsblock->block_size) {
            LOG(WARNING, "block #%ld size mismatch", id);
            assert(0);
            nsblock->replica.erase(server_id);
            return false;
        } else {
            LOG(INFO, "block #%ld size update by C%d V%ld ,%ld to %ld",
                id, server_id, block_version, nsblock->block_size, block_size);
            nsblock->block_size = block_size;
        }
    } else {
        //LOG(DEBUG, "UpdateBlockInfo(%ld) ignored, from %ld to %ld",
        //    id, nsblock->block_size, block_size);
    }
    if (nsblock->replica.size() == 0) {
        lost_blocks_.erase(id);
    }
    std::pair<std::set<int32_t>::iterator, bool> ret = nsblock->replica.insert(server_id);
    int32_t cur_replica_num = nsblock->replica.size();
    int32_t expect_replica_num = nsblock->expect_replica_num;
    bool clean_redundancy = false;
    if (clean_redundancy && cur_replica_num > expect_replica_num) {
        LOG(INFO, "Too much replica #%ld cur=%d expect=%d server=C%d ",
            id, cur_replica_num, expect_replica_num, server_id);
        nsblock->replica.erase(ret.first);
        return false;
    } else {
        if (ret.second) {
            LOG(DEBUG, "New replica C%d for #%ld total: %d",
                server_id, id, cur_replica_num);
        }
        if (cur_replica_num < expect_replica_num && nsblock->version >= 0) {
            if (!safe_mode && !nsblock->pending_recover) {
                LOG(DEBUG, "UpdateBlock #%ld by C%d rep_num %d, add to recover",
                    id, server_id, cur_replica_num);
                AddToRecover(nsblock);
            }
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
    NSBlock* block = it->second;
    if (block->incomplete) {
        incomplete_blocks_.erase(block_id);
    }
    if (block->replica.size() == 0) {
        lost_blocks_.erase(block_id);
    }
    if (block->replica.size() == 1) {
        hi_pri_recover_.erase(block_id);
    }
    delete block;
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
        LOG(INFO, "FinishBlock update version from %ld to %ld",
            it->second->version, version);
        it->second->version = version;
    }
    return ret;
}

void BlockMapping::DealWithDeadBlocks(int32_t cs_id, const std::set<int64_t>& blocks) {
    for (std::set<int64_t>::iterator it = blocks.begin(); it != blocks.end(); ++it) {
        MutexLock lock(&mu_);
        NSBlock* block = NULL;
        int64_t block_id = *it;
        if (!GetBlockPtr(block_id, &block)) {
            LOG(DEBUG, "DealWithDeadBlocks for C%d can't find block: #%ld ", cs_id, block_id);
            continue;
        }
        block->replica.erase(cs_id);
        int32_t rep_num = block->replica.size();
        if (rep_num < block->expect_replica_num) {
            if (rep_num == 0) {
                hi_pri_recover_.erase(block_id);
                lost_blocks_.insert(block_id);
                LOG(INFO, "Block #%ld lost all replica", block_id);
                continue;
            }
            if (block->version == -1) {
                LOG(INFO, "Incomplete block #%ld at C%d, don't recover",
                    block_id, cs_id);
                if (!block->incomplete) {
                    block->incomplete = true;
                    incomplete_blocks_.insert(block_id);
                } else {
                    LOG(WARNING, "Urgent incomplete block #%ld replica= %lu",
                        block_id, block->replica.size());
                }
                continue;
            }
            LOG(DEBUG, "DeadBlock #%ld at C%d , add to recover rep_num: %d",
                block_id, cs_id, rep_num);
            AddToRecover(block);
        }
    }
}

void BlockMapping::PickRecoverBlocks(int32_t cs_id, int32_t block_num,
                                     std::map<int64_t, int32_t>* recover_blocks) {
    MutexLock lock(&mu_);
    std::vector<std::pair<int32_t, int64_t> > tmp_holder;
    CheckList::iterator check_it = recover_check_.insert(std::make_pair(cs_id, std::set<int64_t>())).first;
    int32_t quota = FLAGS_recover_speed - (check_it->second).size();
    LOG(DEBUG, "C%d has %lu pending_recover blocks", cs_id, (check_it->second).size());
    quota = quota < block_num ? quota : block_num;
    while (static_cast<int>(recover_blocks->size()) < quota && !recover_q_.empty()) {
        std::pair<int64_t, int64_t> recover_item = recover_q_.top();
        NSBlock* cur_block = NULL;
        if (!GetBlockPtr(recover_item.second, &cur_block)) { // block is removed
            LOG(DEBUG, "PickRecoverBlocks for C%d can't find block: #%ld ",
                cs_id, recover_item.second);
            recover_q_.pop();
            continue;
        }
        if (static_cast<int32_t>(cur_block->replica.size()) >= cur_block->expect_replica_num) {
            LOG(DEBUG, "Replica num enough #%ld %lu", cur_block->id, cur_block->replica.size());
            cur_block->pending_recover = false;
            recover_q_.pop();
            continue;
        }
        if (cur_block->replica.size() == 0) {
            LOG(DEBUG, "All Replica lost #%ld , give up recover.", cur_block->id);
            cur_block->pending_recover = false;
            recover_q_.pop();
            continue;
        }
        if (cur_block->replica.find(cs_id) != cur_block->replica.end()) {
            tmp_holder.push_back(recover_item);
            recover_q_.pop();
            continue;
        }
        int src_id = *(cur_block->replica.begin());
        recover_blocks->insert(std::make_pair(cur_block->id, src_id));
        (check_it->second).insert(cur_block->id);
        LOG(INFO, "PickRecoverBlocks for C%d #%ld source: C%d ",
            cs_id, cur_block->id, src_id);
        thread_pool_.DelayTask(FLAGS_recover_timeout * 1000,
            boost::bind(&BlockMapping::CheckRecover, this, cs_id, cur_block->id));
        recover_q_.pop();
    }
    for (std::vector<std::pair<int32_t, int64_t> >::iterator it = tmp_holder.begin();
         it != tmp_holder.end(); ++it) {
        recover_q_.push(*it);
    }
    LOG(DEBUG, "recover_q_ size = %lu", recover_q_.size());
}

void BlockMapping::ProcessRecoveredBlock(int32_t cs_id, int64_t block_id, bool recover_success) {
    MutexLock lock(&mu_);
    NSBlock* block = NULL;
    if (!GetBlockPtr(block_id, &block)) {
        LOG(DEBUG, "ProcessRecoveredBlock for C%d can't find block: #%ld ", cs_id, block_id);
        block = NULL;
    }
    if (recover_success) {
        if (block) {
            block->replica.insert(cs_id);
        }
        LOG(DEBUG, "Recovered block #%ld at C%d ", block_id, cs_id);
    } else {
        LOG(INFO, "Recover block fail #%ld at C%d", block_id, cs_id);
    }
    CheckList::iterator it = recover_check_.find(cs_id);
    if (it == recover_check_.end()) {
        LOG(DEBUG, "Not in recover_check_ #%ld C%d ", block_id, cs_id);
        return;
    }
    (it->second).erase(block_id);
    if (block) {
        TryRecover(block);
    }
}

void BlockMapping::GetStat(int64_t* recover_num, int64_t* pending_num,
                           int64_t* urgent_num, int64_t* lost_num,
                           int64_t* incomplete_num) {
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
    if (urgent_num) {
        *urgent_num = hi_pri_recover_.size();
    }
    if (lost_num) {
        *lost_num = lost_blocks_.size();
    }
    if (incomplete_num) {
        *incomplete_num = incomplete_blocks_.size();
    }
}

void BlockMapping::AddToRecover(NSBlock* block) {
    mu_.AssertHeld();
    int64_t block_id = block->id;
    if (!block->pending_recover) {
        recover_q_.push(std::make_pair(block->expect_replica_num - block->replica.size(), block_id));
        block->pending_recover = true;
    } else {
        LOG(DEBUG, "Pending recover #%ld replica=%ld", block_id, block->replica.size());
    }
    if (block->replica.size() == 1) {
        hi_pri_recover_.insert(block_id);
    }
}

void BlockMapping::TryRecover(NSBlock* block) {
    mu_.AssertHeld();
    int64_t block_id = block->id;
    int64_t replica_diff = block->expect_replica_num - block->replica.size();
    if (replica_diff > 0) {
        recover_q_.push(std::make_pair(replica_diff, block_id));
        if (block->replica.size() == 1) {
            hi_pri_recover_.insert(block_id);
        } else {
            hi_pri_recover_.erase(block_id);
        }
        LOG(DEBUG, "need more recover: #%ld pending %d", block_id, block->pending_recover);
    } else {
        block->pending_recover = false;
        hi_pri_recover_.erase(block_id);
        LOG(DEBUG, "recover done: #%ld ", block_id);
    }
}

void BlockMapping::CheckRecover(int64_t cs_id, int64_t block_id) {
    MutexLock lock(&mu_);
    LOG(DEBUG, "recover timeout check: #%ld C%d ", block_id, cs_id);
    CheckList::iterator it = recover_check_.find(cs_id);
    if (it == recover_check_.end()) return;
    std::set<int64_t>& block_set = it->second;
    std::set<int64_t>::iterator check_it = block_set.find(block_id);
    if (check_it == block_set.end()) {
        return;
    }
    NSBlock* block = NULL;
    if (!GetBlockPtr(block_id, &block)) {
        LOG(DEBUG, "CheckRecover for C%d can't find block: #%ld ", cs_id, block_id);
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
