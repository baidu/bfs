// Copyright (c) 2014, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "block_mapping.h"

#include <vector>
#include <boost/bind.hpp>
#include <gflags/gflags.h>

#include <common/logging.h>
#include <common/string_util.h>

DECLARE_int32(recover_speed);
DECLARE_int32(recover_timeout);
DECLARE_bool(bfs_bug_tolerant);
DECLARE_bool(clean_redundancy);

namespace baidu {
namespace bfs {

NSBlock::NSBlock()
    : id(-1), version(-1), block_size(-1),
      expect_replica_num(0), recover_stat(kNotInRecover) {
}
NSBlock::NSBlock(int64_t block_id, int32_t replica,
                 int64_t block_version, int64_t block_size)
    : id(block_id), version(block_version),
      block_size(block_size), expect_replica_num(replica),
      recover_stat(block_version < 0 ? kBlockWriting : kNotInRecover) {
}

BlockMapping::BlockMapping() : next_block_id_(1), safe_mode_(true) {}

void BlockMapping::SetSafeMode(bool safe_mode) {
    safe_mode_ = safe_mode;
}

int64_t BlockMapping::NewBlockID() {
    MutexLock lock(&mu_, "BlockMapping::NewBlockID", 1000);
    return next_block_id_++;
}

bool BlockMapping::GetBlock(int64_t block_id, NSBlock* block) {
    MutexLock lock(&mu_, "BlockMapping::GetBlock", 1000);
    NSBlock* nsblock = NULL;
    if (!GetBlockPtr(block_id, &nsblock)) {
        LOG(WARNING, "GetBlock can not find block: #%ld", block_id);
        return false;
    }
    if (block) {
        *block = *nsblock;
    }
    return true;
}

bool BlockMapping::GetLocatedBlock(int64_t id, std::vector<int32_t>* replica ,int64_t* size) {
    MutexLock lock(&mu_);
    NSBlock* block = NULL;
    if (!GetBlockPtr(id, &block)) {
        LOG(WARNING, "GetReplicaLocation can not find block: #%ld ", id);
        return false;
    }
    replica->assign(block->replica.begin(), block->replica.end());
    if (block->recover_stat == kBlockWriting
        || block->recover_stat == kIncomplete) {
        LOG(DEBUG, "GetLocatedBlock return writing block #%ld ", id);
        replica->insert(replica->end(),
                        block->incomplete_replica.begin(),
                        block->incomplete_replica.end());
    }
    *size = block->block_size;
    return true;
}

bool BlockMapping::ChangeReplicaNum(int64_t block_id, int32_t replica_num) {
    MutexLock lock(&mu_);
    NSBlockMap::iterator it = block_map_.find(block_id);
    NSBlock* block = NULL;
    if (!GetBlockPtr(block_id, &block)) {
        LOG(WARNING, "Can't find block: #%ld ", block_id);
        return false;
    }
    block->expect_replica_num = replica_num;
    return true;
}

void BlockMapping::AddNewBlock(int64_t block_id, int32_t replica,
                               int64_t version, int64_t size,
                               const std::vector<int32_t>* init_replicas) {
    NSBlock* nsblock = NULL;
    nsblock = new NSBlock(block_id, replica, version, size);
    if (init_replicas) {
        if (nsblock->recover_stat == kNotInRecover) {
            nsblock->replica.insert(init_replicas->begin(), init_replicas->end());
        } else {
            nsblock->incomplete_replica.insert(init_replicas->begin(), init_replicas->end());
        }
        LOG(DEBUG, "Init block info: #%ld ", block_id);
    } else {
        LOG(DEBUG, "Rebuild block #%ld V%ld %ld", block_id, version, size);
    }

    MutexLock lock(&mu_);
    std::pair<NSBlockMap::iterator, bool> ret =
        block_map_.insert(std::make_pair(block_id,nsblock));
    assert(ret.second == true);
    if (next_block_id_ <= block_id) {
        next_block_id_ = block_id + 1;
    }
}

bool BlockMapping::UpdateWritingBlock(NSBlock* nsblock,
                                      int32_t cs_id, int64_t block_size,
                                      int64_t block_version) {
    int64_t block_id = nsblock->id;
    std::set<int32_t>& inc_replica = nsblock->incomplete_replica;
    std::set<int32_t>& replica = nsblock->replica;
    if (block_version < 0) {
        if (replica.find(cs_id) != replica.end()) return true; // out-of-order message
        if (inc_replica.insert(cs_id).second) {
            LOG(INFO, "New replica C%d V%ld %ld for writing block #%ld IR%lu",
                cs_id, block_version, block_size, block_id, inc_replica.size());
        }
        return true;
    }
    /// Then block_version > 0
    inc_replica.erase(cs_id);
    if (block_version > nsblock->version) { // First received block or new block version
        LOG(INFO, "Block #%ld update by C%d from V%ld %ld to V%ld %ld",
            block_id, cs_id, nsblock->version, nsblock->block_size,
            block_version, block_size);
        ///TODO: if nsblock->version > 0, clean replica and trigger recover
        nsblock->version = block_version;
        nsblock->block_size = block_size;
    } else if (block_version < nsblock->version) {    // block_version mismatch
        replica.erase(cs_id);
        LOG(INFO, "Block #%ld C%d has old version V%ld %ld now: V%ld %ld R%lu IR%lu",
            block_id, cs_id, block_version, block_size,
            nsblock->version, nsblock->block_size, replica.size(), inc_replica.size());
        if (safe_mode_) return true;
        if (replica.empty() && inc_replica.empty()) {
            LOG(WARNING, "Data lost #%ld C%d V%ld %ld -> V%ld %ld",
                block_id, cs_id, block_version, block_size,
                nsblock->version, nsblock->block_size);
            nsblock->version = block_version;
            nsblock->block_size = block_size;
        } else {
            if (inc_replica.empty()) {
                SetState(nsblock, kNotInRecover);
                TryRecover(nsblock);
                if (replica.size() >= 2) {
                    LOG(DEBUG, "Drop replica #%ld C%d V%ld R%lu IR%lu",
                        block_id, cs_id, block_version ,
                        replica.size(), inc_replica.size());
                    return false;
                }
            } else {
                SetState(nsblock, kIncomplete);
                InsertToIncomplete(block_id, inc_replica);
            }
            LOG(INFO, "Keep replica #%ld C%d V%ld R%lu IR%lu",
                block_id, cs_id, block_version, replica.size(), inc_replica.size());
            return true;
        }
    } else { //block_version == nsblock->version) Another received block
        if (block_size != nsblock->block_size) {
            LOG(WARNING, "Block #%ld V%ld size mismatch, old: %ld new: %ld",
                block_id, block_version, nsblock->block_size, block_size);
            replica.erase(cs_id);
            if (!FLAGS_bfs_bug_tolerant) abort();
            return false;
        }
    }

    if (replica.insert(cs_id).second) {
        LOG(INFO, "Writing replica finish #%ld C%d V%ld %ld R%lu IR%lu",
            block_id, cs_id, block_version, block_size, replica.size(), inc_replica.size());
    }
    if (inc_replica.empty()
        && (!safe_mode_ || replica.size() >= nsblock->expect_replica_num)) {
        LOG(INFO, "Writing block complete #%ld V%ld %ld R%lu",
            block_id, block_version, block_size, replica.size());
        SetState(nsblock, kNotInRecover);
        TryRecover(nsblock);
    }
    return true;
}
bool BlockMapping::UpdateNormalBlock(NSBlock* nsblock,
                                      int32_t cs_id, int64_t block_size,
                                      int64_t block_version) {
    int64_t block_id = nsblock->id;
    std::set<int32_t>& inc_replica = nsblock->incomplete_replica;
    std::set<int32_t>& replica = nsblock->replica;
    if (block_version < 0) {
        if (nsblock->recover_stat == kCheck) {
            return true;
        } else {
            // handle out-of-order message
            if (replica.find(cs_id) != replica.end()) {
                return true;
            }
            LOG(WARNING, "Incomplete block #%ld from C%d drop it, now V%ld %ld R%lu",
                block_id, cs_id, nsblock->version, nsblock->block_size, replica.size());
            return false;
        }
    }
    /// Then block_version >= 0
    if (block_version > nsblock->version) {
        LOG(INFO, "Block #%ld update by C%d from V%ld %ld to V%ld %ld",
            block_id, cs_id, nsblock->version, nsblock->block_size,
            block_version, block_size);
        nsblock->version = block_version;
        nsblock->block_size = block_size;
    } else if (block_version < nsblock->version) {
        replica.erase(cs_id);
        LOG(INFO, "Block #%ld C%d has old version V%ld %ld now: V%ld %ld R%lu IR%lu",
            block_id, cs_id, block_version, block_size,
            nsblock->version, nsblock->block_size, replica.size(), inc_replica.size());
        if (safe_mode_) return true;
        if (replica.empty()) {
            LOG(WARNING, "Data lost #%ld C%d V%ld %ld -> V%ld %ld",
                block_id, cs_id, block_version, block_size,
                nsblock->version, nsblock->block_size);
            nsblock->version = block_version;
            nsblock->block_size = block_size;
        } else {
            TryRecover(nsblock);
            if (replica.size() >= 2) {
                LOG(INFO, "Drop replica #%ld C%d V%ld R%lu",
                    block_id, cs_id, block_version , replica.size());
                return false;
            } else {
                LOG(INFO, "Keep replica #%ld C%d V%ld R%lu",
                    block_id, cs_id, block_version, replica.size());
                return true;
            }
        }
    } else { //block_version == nsblock->version, normal block report
        if (block_size != nsblock->block_size) {
            LOG(WARNING, "Block #%ld V%ld size mismatch, old: %ld new: %ld",
                block_id, block_version, nsblock->block_size, block_size);
            replica.erase(cs_id);
            if (!FLAGS_bfs_bug_tolerant) abort();
            return false;
        }
    }

    if (replica.insert(cs_id).second) {
        LOG(INFO, "New replica C%d V%ld %ld for #%ld R%lu",
            cs_id, block_version, block_size, block_id, replica.size());
    }

    if (!safe_mode_ || replica.size() >= nsblock->expect_replica_num) {
        TryRecover(nsblock);
    }
    if (FLAGS_clean_redundancy && !safe_mode_ && replica.size() > nsblock->expect_replica_num) {
        LOG(INFO, "Too much replica #%ld R%lu expect=%d C%d ",
            block_id, replica.size(), nsblock->expect_replica_num, cs_id);
        replica.erase(cs_id);
        return false;
    }
    return true;
}

bool BlockMapping::UpdateIncompleteBlock(NSBlock* nsblock,
                                         int32_t cs_id, int64_t block_size,
                                         int64_t block_version) {
    int64_t block_id = nsblock->id;
    std::set<int32_t>& inc_replica = nsblock->incomplete_replica;
    std::set<int32_t>& replica = nsblock->replica;
    if (block_version < 0) {
        // handle out-of-order message
        if (replica.find(cs_id) == replica.end()) {
            if (inc_replica.insert(cs_id).second) {
                LOG(INFO, "New incomplete replica #%ld C%d V%ld %ld R%lu IR%lu",
                    block_id, cs_id, block_version, block_size,
                    replica.size(), inc_replica.size());
                incomplete_[cs_id].insert(block_id);
            }
        }
        return true;
    }
    /// Then block_version >= 0
    if (inc_replica.erase(cs_id)) {
        RemoveFromIncomplete(block_id, cs_id);
    }
    if (block_version > nsblock->version) {
        LOG(INFO, "Block #%ld update by C%d from V%ld %ld to V%ld %ld",
            block_id, cs_id, nsblock->version, nsblock->block_size,
            block_version, block_size);
        nsblock->version = block_version;
        nsblock->block_size = block_size;
    } else if (block_version < nsblock->version) {
        replica.erase(cs_id);
        LOG(INFO, "Block #%ld C%d has old version V%ld %ld now: V%ld %ld R%lu IR%lu",
            block_id, cs_id, block_version, block_size,
            nsblock->version, nsblock->block_size, replica.size(), inc_replica.size());
        if (safe_mode_) return true;
        if (replica.empty() && inc_replica.empty()) {
            LOG(WARNING, "Data lost #%ld C%d V%ld %ld -> V%ld %ld",
                block_id, cs_id, block_version, block_size,
                nsblock->version, nsblock->block_size);
            nsblock->version = block_version;
            nsblock->block_size = block_size;
        } else {
            if (inc_replica.empty()) {
                SetState(nsblock, kNotInRecover);
                TryRecover(nsblock);
                if (replica.size() >= 2) {
                    LOG(DEBUG, "Drop replica #%ld C%d V%ld R%lu IR%lu",
                        block_id, cs_id, block_version ,
                        replica.size(), inc_replica.size());
                    return false;
                }
            }
            LOG(DEBUG, "Keep replica #%ld C%d V%ld R%lu IR%lu",
                block_id, cs_id, block_version ,
                replica.size(), inc_replica.size());
            return true;
        }
    } else { //block_version == nsblock->version
        if (block_size != nsblock->block_size) {
            LOG(WARNING, "Block #%ld V%ld size mismatch, old: %ld new: %ld",
                block_id, block_version, nsblock->block_size, block_size);
            replica.erase(cs_id);
            if (!FLAGS_bfs_bug_tolerant) abort();
            return false;
        }
    }

    if (replica.insert(cs_id).second) {
        LOG(INFO, "Incomplete block replica finish %ld C%d V%ld %ld R%lu IR%lu",
            block_id, cs_id, block_version, block_size, replica.size(), inc_replica.size());
    }
    if (inc_replica.empty()
        && (!safe_mode_ || replica.size() >= nsblock->expect_replica_num)) {
        LOG(INFO, "Incomplete block complete #%ld V%ld %ld R%lu",
            block_id, block_version, block_size, replica.size());
        SetState(nsblock, kNotInRecover);
        TryRecover(nsblock);
    }
    return true;
}

/*
bool BlockMapping::UpdateBlockInfoMerge(NSBlock* nsblock,
                                         int32_t cs_id, int64_t block_size,
                                         int64_t block_version, bool safe_mode) {
    int64_t block_id = nsblock->id;
    std::set<int32_t>& inc_replica = nsblock->incomplete_replica;
    std::set<int32_t>& replica = nsblock->replica;
    RecoverStat stat = nsblock->recover_stat;
    if (block_version < 0) {
        if (stat == kBlockWriting) {
            if (replica.find(cs_id) != replica.end()) return true; // out-of-order message
            if (inc_replica.insert(cs_id).second) {
                LOG(INFO, "New replica C%d V%ld %ld for writing block #%ld R%lu",
                    cs_id, block_version, block_size, block_id, inc_replica.size());
            }
            return true;
        } else if (stat == kIncomplete) {
            inc_replica.insert(cs_id);
        } else if (stat == kCheck) {
            return true;
        } else {
            ///TODO: handle out-of-order message
            LOG(WARNING, "Incomplete block #%ld from C%d drop it, now V%ld %ld R%lu",
                block_id, cs_id, nsblock->version, nsblock->block_size, replica.size());
            replica.erase(cs_id);
            return false;
        }
        return true;
    }
    /// Then block_version >= 0
    if (inc_replica.erase(cs_id)) {
        if (stat == kBlockWriting) {
        } else if (stat == kIncomplete) {
            RemoveFromIncomplete(block_id, cs_id);
        } else {
            assert(0);
        }
    }
    if (block_version > nsblock->version) { // First received block or new block version
        LOG(INFO, "Block #%ld update by C%d from V%ld %ld to V%ld %ld",
            block_id, cs_id, nsblock->version, nsblock->block_size,
            block_version, block_size);
        ///TODO: if nsblock->version > 0, clean replica and trigger recover
        nsblock->version = block_version;
        nsblock->block_size = block_size;
     } else if (block_version < nsblock->version) {    // block_version mismatch
        replica.erase(cs_id);
        LOG(INFO, "Block #%ld C%d has old version V%ld %ld now: V%ld %ld R%lu",
            block_id, cs_id, block_version, block_size,
            nsblock->version, nsblock->block_size, replica.size());
        if (safe_mode) return true;
        if (replica.empty() && inc_replica.empty()) {
            LOG(WARNING, "Data lost #%ld C%d V%ld %ld -> V%ld %ld",
                block_id, cs_id, block_version, block_size,
                nsblock->version, nsblock->block_size);
            nsblock->version = block_version;
            nsblock->block_size = block_size;
        } else {
            if (inc_replica.empty()) {
                if (stat != kNotInRecover) {
                    SetState(nsblock, kNotInRecover);
                }
                TryRecover(nsblock);
                if (replica.size() >= 2) {
                    LOG(DEBUG, "Drop replica #%ld C%d V%ld R%lu IR%lu",
                        block_id, cs_id, block_version ,
                        replica.size(), inc_replica.size());
                    return false;
                }
            } else if (stat != kIncomplete) {
                SetState(nsblock, kIncomplete);
                InsertToIncomplete(block_id, inc_replica);
            }
            LOG(INFO, "Keep replica #%ld C%d V%ld R%lu IR%lu",
                block_id, cs_id, block_version, replica.size(), inc_replica.size());
            return true;
        }
    } //else { block_version == nsblock->version) Another received block


    if (replica.insert(cs_id).second) {
        LOG(INFO, "Writing replica finish #%ld C%d V%ld %ld R%lu IR%lu",
            block_id, cs_id, block_version, block_size, replica.size(), inc_replica.size());
    }
    if (!safe_mode || replica.size() >= nsblock->expect_replica_num) {
        if (inc_replica.empty()) {
            if (stat == kBlockWriting) {
                LOG(INFO, "Writing block complete #%ld V%ld %ld R%lu",
                    block_id, block_version, block_size, replica.size());
                SetState(nsblock, kNotInRecover);
            } else (stat == kIncomplete) {
                LOG(INFO, "Incomplete block complete #%ld V%ld %ld R%lu",
                    block_id, block_version, block_size, replica.size());
                SetState(nsblock, kNotInRecover);
            }
            TryRecover(nsblock);
        }
    }
    return true;
}
*/
bool BlockMapping::UpdateBlockInfo(int64_t block_id, int32_t server_id, int64_t block_size,
                                   int64_t block_version) {
    MutexLock lock(&mu_);
    NSBlock* block = NULL;
    if (!GetBlockPtr(block_id, &block)) {
        LOG(DEBUG, "UpdateBlockInfo C%d #%ld has been removed", server_id, block_id);
        return false;
    }
    switch (block->recover_stat) {
      case kBlockWriting:
        return UpdateWritingBlock(block, server_id, block_size, block_version);
      case kIncomplete:
        return UpdateIncompleteBlock(block, server_id, block_size, block_version);
      default:  // kNotInRecover kLow kHi kLost kCheck
        return UpdateNormalBlock(block, server_id, block_size, block_version);
    }
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
    NSBlock* block = NULL;
    if (!GetBlockPtr(block_id, &block)) {
        LOG(WARNING, "RemoveBlock #%ld not found", block_id);
        return;
    }
    if (block->recover_stat == kIncomplete) {
        for (std::set<int32_t>::iterator it = block->incomplete_replica.begin();
             it != block->incomplete_replica.end(); ++it) {
            RemoveFromIncomplete(block_id, *it);
        }
    } else if (block->recover_stat == kLost) {
        lost_blocks_.erase(block_id);
    } else if (block->recover_stat == kHiRecover) {
        hi_pri_recover_.erase(block_id);
    } else if (block->recover_stat == kLoRecover) {
        lo_pri_recover_.erase(block_id);
    }
    delete block;
    block_map_.erase(block_id);
}

StatusCode BlockMapping::CheckBlockVersion(int64_t block_id, int64_t version) {
    MutexLock lock(&mu_);
    NSBlock* block = NULL;
    if (!GetBlockPtr(block_id, &block)) {
        LOG(WARNING, "CheckBlockVersion can not find block: #%ld ", block_id);
        return kNotFound;
    }
    if (block->version != version) {
        LOG(WARNING, "CheckBlockVersion fail #%ld V%ld to V%ld",
            block_id, block->version, version);
        return kVersionError;
    }
    return kOK;
}

void BlockMapping::DealWithDeadBlock(int32_t cs_id, int64_t block_id) {
    mu_.AssertHeld();
    NSBlock* block = NULL;
    if (!GetBlockPtr(block_id, &block)) {
        LOG(DEBUG, "DealWithDeadBlocks for C%d can't find block: #%ld ", cs_id, block_id);
        return;
    }
    std::set<int32_t>& inc_replica = block->incomplete_replica;
    std::set<int32_t>& replica = block->replica;
    if (inc_replica.erase(cs_id)) {
        if (block->recover_stat == kIncomplete) {
            RemoveFromIncomplete(block_id, cs_id);
        } else if (block->recover_stat == kCheck) {
            LOG(INFO, "Recovering replica dead #%ld C%d R%lu IR%lu retry recover",
                block_id, cs_id, replica.size(), inc_replica.size());
            SetState(block, kNotInRecover);
            //RemoveFromRecoverCheckList(cs_id, block_id);
        } // else kBlockWriting
    } else {
        if (replica.erase(cs_id) == 0) {
            LOG(INFO, "Dead replica C%d #%ld not in blockmapping, ignore it R%lu IR%lu",
                cs_id, block_id, replica.size(), inc_replica.size());
            return;
        }
    }
    if (block->recover_stat == kIncomplete) {
        LOG(INFO, "Incomplete block C%d #%ld dead replica= %lu",
            cs_id, block_id, replica.size());
        if (inc_replica.empty() && !safe_mode_) SetState(block, kNotInRecover);
    } else if (block->recover_stat == kBlockWriting) {
        LOG(INFO, "Writing block C%d #%ld dead R%lu IR%lu",
            cs_id, block_id, replica.size(), inc_replica.size());
        if (inc_replica.size() > 0) {
            SetState(block, kIncomplete);
            InsertToIncomplete(block_id, inc_replica);
        } else {
            if (!safe_mode_) {
                SetState(block, kNotInRecover);
            }
        }
    }   // else Normal check low hi

    LOG(DEBUG, "Dead replica at C%d add #%ld R%lu try recover",
        cs_id, block_id, replica.size());
    TryRecover(block);
}

void BlockMapping::DealWithDeadNode(int32_t cs_id, const std::set<int64_t>& blocks) {
    for (std::set<int64_t>::iterator it = blocks.begin(); it != blocks.end(); ++it) {
        MutexLock lock(&mu_);
        DealWithDeadBlock(cs_id, *it);
    }
    MutexLock lock(&mu_);
    for (std::set<int64_t>::iterator it = hi_recover_check_[cs_id].begin();
            it != hi_recover_check_[cs_id].end(); ++it) {
         DealWithDeadBlock(cs_id, *it);
    }
    hi_recover_check_.erase(cs_id);
    for (std::set<int64_t>::iterator it = lo_recover_check_[cs_id].begin();
            it != lo_recover_check_[cs_id].end(); ++it) {
        DealWithDeadBlock(cs_id, *it);
    }
    lo_recover_check_.erase(cs_id);
}

void BlockMapping::PickRecoverBlocks(int32_t cs_id, int32_t block_num,
                                     std::map<int64_t, int32_t>* recover_blocks,
                                     int32_t* hi_num) {
    MutexLock lock(&mu_);
    if (hi_pri_recover_.empty() && lo_pri_recover_.empty()) {
        return;
    }
    std::set<int64_t>& lo_check_set = lo_recover_check_[cs_id];
    std::set<int64_t>& hi_check_set = hi_recover_check_[cs_id];
    int32_t quota = FLAGS_recover_speed - lo_check_set.size() - hi_check_set.size();
    LOG(DEBUG, "C%d has %lu/%lu pending_recover blocks",
        cs_id, hi_check_set.size(), lo_check_set.size());
    quota = quota < block_num ? quota : block_num;
    LOG(DEBUG, "Before Pick: recover num(hi/lo): %ld/%ld ",
        hi_pri_recover_.size(), lo_pri_recover_.size());
    PickRecoverFromSet(cs_id, quota, &hi_pri_recover_, recover_blocks, &hi_check_set);
    if (hi_num) *hi_num = recover_blocks->size();
    PickRecoverFromSet(cs_id, quota, &lo_pri_recover_, recover_blocks, &lo_check_set);
    LOG(DEBUG, "After Pick: recover num(hi/lo): %ld/%ld ", hi_pri_recover_.size(), lo_pri_recover_.size());
    LOG(INFO, "C%d picked %lu blocks to recover", cs_id, recover_blocks->size());
}

bool BlockMapping::RemoveFromRecoverCheckList(int32_t cs_id, int64_t block_id) {
    mu_.AssertHeld();
    std::set<int64_t>::iterator it = lo_recover_check_[cs_id].find(block_id);
    if (it != lo_recover_check_[cs_id].end()){
        lo_recover_check_[cs_id].erase(it);
        //LOG(DEBUG, "Remove #%ld from lo_recover_check", block_id);
        if (lo_recover_check_[cs_id].empty()) {
            lo_recover_check_.erase(cs_id);
        }
    } else {
        it = hi_recover_check_[cs_id].find(block_id);
        if (it != hi_recover_check_[cs_id].end()) {
            //LOG(DEBUG, "Remove #%ld from hi_recover_check", block_id);
            hi_recover_check_[cs_id].erase(it);
            if (hi_recover_check_[cs_id].empty()) {
                hi_recover_check_.erase(cs_id);
            }
        } else {
            return false;
        }
    }
    return true;
}
void BlockMapping::ProcessRecoveredBlock(int32_t cs_id, int64_t block_id, bool recover_success) {
    MutexLock lock(&mu_);
    bool ret = RemoveFromRecoverCheckList(cs_id, block_id);
    NSBlock* block = NULL;
    if (!GetBlockPtr(block_id, &block)) {
        LOG(DEBUG, "ProcessRecoveredBlock for C%d can't find block: #%ld ", cs_id, block_id);
        return;
    }
    if (ret) {
        block->incomplete_replica.erase(cs_id);
        SetState(block, kNotInRecover);
    } else {
        LOG(WARNING, "RemoveFromRecoverCheckList fail #%ld C%d %s",
            block_id, cs_id, RecoverStat_Name(block->recover_stat).c_str());
    }
    if (recover_success) {
        block->replica.insert(cs_id);
        LOG(DEBUG, "Recovered block #%ld at C%d ", block_id, cs_id);
    } else {
        LOG(INFO, "Recover block fail #%ld at C%d ", block_id, cs_id);
    }
    TryRecover(block);
}

void BlockMapping::GetCloseBlocks(int32_t cs_id,
                                  google::protobuf::RepeatedField<int64_t>* close_blocks) {
    MutexLock lock(&mu_);
    CheckList::iterator c_it = incomplete_.find(cs_id);
    if (c_it != incomplete_.end()) {
        const std::set<int64_t>& blocks = c_it->second;
        for (std::set<int64_t>::iterator it = blocks.begin(); it != blocks.end(); ++it) {
            LOG(INFO, "GetCloseBlocks #%ld at C%d ", *it, cs_id);
            close_blocks->Add(*it);
        }
    }
}

void BlockMapping::GetStat(int64_t* lo_recover_num, int64_t* hi_recover_num,
                           int64_t* lo_pending, int64_t* hi_pending,
                           int64_t* lost_num, int64_t* incomplete_num) {
    MutexLock lock(&mu_);
    if (lo_recover_num) {
        *lo_recover_num = lo_pri_recover_.size();
    }
    if (lo_pending) {
        *lo_pending = 0;
        for (CheckList::iterator it = lo_recover_check_.begin(); it != lo_recover_check_.end(); ++it) {
            *lo_pending += (it->second).size();
        }
    }
    if (hi_pending) {
        *hi_pending = 0;
        for (CheckList::iterator it = hi_recover_check_.begin(); it != hi_recover_check_.end(); ++it) {
            *hi_pending += (it->second).size();
        }
    }
    if (hi_recover_num) {
        *hi_recover_num = hi_pri_recover_.size();
    }
    if (lost_num) {
        *lost_num = lost_blocks_.size();
    }
    if (incomplete_num) {
        *incomplete_num = 0;
        for (CheckList::iterator it = incomplete_.begin(); it != incomplete_.end(); ++it) {
            *incomplete_num += (it->second).size();
        }
    }
}

void BlockMapping::ListCheckList(const CheckList& check_list, std::string* output) {
    for (CheckList::const_iterator it = check_list.begin(); it != check_list.end(); ++it) {
        output->append(common::NumToString(it->first) + ": ");
        const std::set<int64_t>& block_set = it->second;
        uint32_t last = output->size();
        for (std::set<int64_t>::iterator block_it = block_set.begin();
                block_it != block_set.end(); ++block_it) {
            output->append(common::NumToString(*block_it) + " ");
            if (output->size() - last > 1024) {
                output->append("...");
                break;
            }
            last = output->size();
        }
        output->append("<br>");
    }
}
void BlockMapping::ListRecover(std::string* hi_recover, std::string* lo_recover,
                               std::string* lost, std::string* hi_check,
                               std::string* lo_check, std::string* incomplete) {
    MutexLock lock(&mu_);
    for (std::set<int64_t>::iterator it = lo_pri_recover_.begin(); it != lo_pri_recover_.end(); ++it) {
        lo_recover->append(common::NumToString(*it) + " ");
        if (lo_recover->size() > 1024) {
            lo_recover->append("...");
            break;
        }
    }

    for (std::set<int64_t>::iterator it = hi_pri_recover_.begin(); it != hi_pri_recover_.end(); ++it) {
        hi_recover->append(common::NumToString(*it) + " ");
        if (hi_recover->size() > 1024) {
            hi_recover->append("...");
            break;
        }
    }

    for (std::set<int64_t>::iterator it = lost_blocks_.begin(); it != lost_blocks_.end(); ++it) {
        lost->append(common::NumToString(*it) + " ");
        if (lost->size() > 1024) {
            lost->append("...");
            break;
        }
    }

    ListCheckList(hi_recover_check_, hi_check);
    ListCheckList(lo_recover_check_, lo_check);
    ListCheckList(incomplete_, incomplete);
}

void BlockMapping::PickRecoverFromSet(int32_t cs_id, int32_t quota,
                                      std::set<int64_t>* recover_set,
                                      std::map<int64_t, int32_t>* recover_blocks,
                                      std::set<int64_t>* check_set) {
    mu_.AssertHeld();
    std::set<int64_t>::iterator it = recover_set->begin();
    while (static_cast<int>(recover_blocks->size()) < quota && it != recover_set->end()) {
        NSBlock* cur_block = NULL;
        if (!GetBlockPtr(*it, &cur_block)) { // block is removed
            LOG(DEBUG, "PickRecoverBlocks for C%d can't find block: #%ld ", cs_id, *it);
            recover_set->erase(it++);
            continue;
        }
        std::set<int32_t>& replica = cur_block->replica;
        int64_t block_id = cur_block->id;
        if (replica.size() >= cur_block->expect_replica_num) {
            LOG(DEBUG, "Replica num enough #%ld %lu", block_id, replica.size());
            recover_set->erase(it++);
            SetState(cur_block, kNotInRecover);
            continue;
        }
        if (replica.size() == 0) {
            LOG(WARNING, "All Replica lost #%ld , give up recover.", block_id);
            abort();
            SetStateIf(cur_block, kAny, kLost);
            lost_blocks_.insert(block_id);
            recover_set->erase(it++);
            continue;
        }
        if (replica.find(cs_id) != replica.end()) {
            ++it;
            continue;
        }
        int index = rand() % replica.size();
        std::set<int32_t>::iterator src_it = replica.begin();
        for (; index >0; index--) {
            ++src_it;
        }
        int src_id = *src_it;
        recover_blocks->insert(std::make_pair(block_id, src_id));
        check_set->insert(block_id);
        cur_block->incomplete_replica.insert(cs_id);
        assert(cur_block->recover_stat == kHiRecover || cur_block->recover_stat == kLoRecover);
        SetState(cur_block, kCheck);
        LOG(INFO, "PickRecoverBlocks for C%d #%ld source: C%d ", cs_id, block_id, src_id);
        thread_pool_.DelayTask(FLAGS_recover_timeout * 1000,
            boost::bind(&BlockMapping::CheckRecover, this, cs_id, block_id));
        recover_set->erase(it++);
    }
}

void BlockMapping::TryRecover(NSBlock* block) {
    mu_.AssertHeld();
    assert (block->recover_stat != kBlockWriting);
    if (safe_mode_ || block->recover_stat == kCheck || block->recover_stat == kIncomplete) {
        return;
    }
    int64_t block_id = block->id;
    if (block->replica.size() < block->expect_replica_num) {
        if (block->replica.size() == 0 && block->recover_stat != kLost) {
            LOG(INFO, "[TryRecover] lost block #%ld ", block_id);
            lost_blocks_.insert(block_id);
            SetState(block, kLost);
            lo_pri_recover_.erase(block_id);
            hi_pri_recover_.erase(block_id);
        } else if (block->replica.size() == 1 && block->recover_stat != kHiRecover) {
            hi_pri_recover_.insert(block_id);
            LOG(INFO, "[TryRecover] need more recover: #%ld %s->kHiRecover",
                block_id, RecoverStat_Name(block->recover_stat).c_str());
            SetState(block, kHiRecover);
            lost_blocks_.erase(block_id);
            lo_pri_recover_.erase(block_id);
        } else if (block->replica.size() > 1 && block->recover_stat != kLoRecover) {
            lo_pri_recover_.insert(block_id);
            LOG(INFO, "[TryRecover] need more recover: #%ld %s->kLoRecover",
                block_id, RecoverStat_Name(block->recover_stat).c_str());
            SetState(block, kLoRecover);
            lost_blocks_.erase(block_id);
            hi_pri_recover_.erase(block_id);
        } // else  Don't change recover_stat
        return;
    }
    if (block->recover_stat != kNotInRecover) {
        LOG(INFO, "Block #%ld V%ld %ld R%lu back to normal from %s",
            block_id, block->version, block->block_size, block->replica.size(),
            RecoverStat_Name(block->recover_stat).c_str());
        SetState(block, kNotInRecover);
        lost_blocks_.erase(block_id);
        hi_pri_recover_.erase(block_id);
        lo_pri_recover_.erase(block_id);
    }
}

void BlockMapping::CheckRecover(int32_t cs_id, int64_t block_id) {
    MutexLock lock(&mu_);
    LOG(DEBUG, "recover timeout check: #%ld C%d ", block_id, cs_id);
    bool ret = RemoveFromRecoverCheckList(cs_id, block_id);
    NSBlock* block = NULL;
    if (!GetBlockPtr(block_id, &block)) {
        LOG(DEBUG, "CheckRecover for C%d can't find block: #%ld ", cs_id, block_id);
        return;
    }
    if (ret) {
        SetState(block, kNotInRecover);
        block->incomplete_replica.erase(cs_id);
    } else {
        // if ret == false, maybe a task for a dead chunkserver, don't change state
        LOG(DEBUG, "CheckRecover not found #%ld C%d %s",
            block_id, cs_id, RecoverStat_Name(block->recover_stat).c_str());
    }
    TryRecover(block);
}

void BlockMapping::InsertToIncomplete(int64_t block_id, const std::set<int32_t>& inc_replica) {
    mu_.AssertHeld();
    std::set<int32_t>::const_iterator cs_it = inc_replica.begin();
    for (; cs_it != inc_replica.end(); ++cs_it) {
        incomplete_[*cs_it].insert(block_id);
        LOG(INFO, "Insert C%d #%ld to incomplete_", *cs_it, block_id);
    }
}
void BlockMapping::RemoveFromIncomplete(int64_t block_id, int32_t cs_id) {
    mu_.AssertHeld();
    bool error = false;
    CheckList::iterator incomplete_it = incomplete_.find(cs_id);
    if (incomplete_it != incomplete_.end()) {
        std::set<int64_t>& incomplete_set = incomplete_it->second;
        if (!incomplete_set.erase(block_id)) error = true;
        if (incomplete_set.empty()) incomplete_.erase(incomplete_it);
    } else {
        error = true;
    }
    if (error) {
        LOG(WARNING, "RemoveFromIncomplete not find C%d #%ld ", cs_id, block_id);
        abort();
    }
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

void BlockMapping::SetState(NSBlock* block, RecoverStat to) {
    mu_.AssertHeld();
    LOG(INFO, "SetState #%ld %s->%s",
        block->id, RecoverStat_Name(block->recover_stat).c_str(),
        RecoverStat_Name(to).c_str());
    block->recover_stat = to;
}
bool BlockMapping::SetStateIf(NSBlock* block, RecoverStat from, RecoverStat to) {
    mu_.AssertHeld();
    if (block->recover_stat == from || from == kAny) {
        SetState(block, to);
        return true;
    }
    return false;
}

} // namespace bfs
} // namespace baidu
