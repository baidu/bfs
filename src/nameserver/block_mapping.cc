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

namespace baidu {
namespace bfs {

NSBlock::NSBlock()
    : id(-1), version(-1), block_size(-1),
      expect_replica_num(0), recover_stat(kNotInRecover) {
}
NSBlock::NSBlock(int64_t block_id, int32_t replica,
                 int64_t block_version, int64_t block_size)
    : id(block_id), version(block_version), block_size(block_size),
      expect_replica_num(replica), recover_stat(kNotInRecover) {
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

bool BlockMapping::GetLocatedBlock(int64_t id, std::vector<int32_t>* replica ,int64_t* size) {
    MutexLock lock(&mu_);
    NSBlockMap::iterator it = block_map_.find(id);
    if (it == block_map_.end()) {
        LOG(WARNING, "GetReplicaLocation can not find block: #%ld ", id);
        return false;
    }
    NSBlock* nsblock = it->second;
    if (nsblock->recover_stat == kBlockWriting) {
        replica->assign(nsblock->incomplete_replica.begin(),
                        nsblock->incomplete_replica.end());
    } else {
        replica->assign(nsblock->replica.begin(), nsblock->replica.end());
    }
    *size = nsblock->block_size;
    return true;
}

bool BlockMapping::ChangeReplicaNum(int64_t block_id, int32_t replica_num) {
    MutexLock lock(&mu_);
    NSBlockMap::iterator it = block_map_.find(block_id);
    if (it == block_map_.end()) {
        LOG(WARNING, "Can't find block: #%ld ", block_id);
        return false;
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
    if (version < 0) {
        nsblock->recover_stat = kBlockWriting;
    }
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
                                      int64_t block_version, bool safe_mode) {
    int64_t block_id = nsblock->id;
    std::set<int32_t>& inc_replica = nsblock->incomplete_replica;
    std::set<int32_t>& replica = nsblock->replica;
    if (block_version < 0) {
        if (replica.find(cs_id) != replica.end()) return true; // out-of-order message
        if (inc_replica.insert(cs_id).second) {
            LOG(INFO, "New replica C%d V%ld %ld for writing block #%ld R%lu",
                cs_id, block_version, block_size, block_id, inc_replica.size());
        }
        return true;
    }
    /// Then block_version > 0
    inc_replica.erase(cs_id);
    if (nsblock->version < 0) { // First received block
        LOG(INFO, "Block #%ld update by C%d from V%ld %ld to V%ld %ld",
            block_id, cs_id, nsblock->version, nsblock->block_size,
            block_version, block_size);
        nsblock->version = block_version;
        nsblock->block_size = block_size;
    } else if (block_version == nsblock->version) { // Another received block
        if (block_size != nsblock->block_size) {
            LOG(WARNING, "Block #%ld V%ld size mismatch, old: %ld new: %ld",
                block_id, block_version, nsblock->block_size, block_size);
            replica.erase(cs_id);
            if (!FLAGS_bfs_bug_tolerant) abort();
            return false;
        }
    } else {    // block_version mismatch
        if (block_version > nsblock->version) {
            nsblock->version = block_version;
            nsblock->block_size = block_size;
            ///TODO: Clean replica and trigger recover
        } else {
            replica.erase(cs_id);
            LOG(INFO, "Drop expired replica C%d V%ld %ld now #%ld V%ld %ld R%lu IR%lu",
                block_id, cs_id, block_version, block_size, nsblock->version,
                nsblock->block_size, replica.size(), inc_replica.size());
            if (nsblock->incomplete_replica.size()) {
                SetState(nsblock, kIncomplete);
            } else {
                SetState(nsblock, kNotInRecover);
                TryRecover(nsblock);
            }
            return false;
        }
    }

    if (replica.insert(cs_id).second) {
        LOG(INFO, "Writing block Replica finish #%ld C%d V%ld %ld R%lu IR%lu",
            block_id, cs_id, block_version, block_size, replica.size(), inc_replica.size());
    }
    if (inc_replica.empty()
        && (!safe_mode || replica.size() > nsblock->expect_replica_num)) {
        LOG(INFO, "Writing block complete #%ld V%ld %ld R%lu",
            block_id, block_version, block_size, replica.size());
        SetState(nsblock, kNotInRecover);
        TryRecover(nsblock);
    }
    return true;
}
bool BlockMapping::UpdateNormalBlock(NSBlock* nsblock,
                                      int32_t cs_id, int64_t block_size,
                                      int64_t block_version, bool safe_mode) {
    int64_t block_id = nsblock->id;
    std::set<int32_t>& replica = nsblock->replica;
    if (block_version < 0) {
        if (nsblock->recover_stat == kCheck) {
            return true;
        } else {
            ///TODO: handle out-of-order message
            LOG(WARNING, "Incomplete block #%ld from C%d drop it, now V%ld %ld R%lu",
                block_id, cs_id, nsblock->version, nsblock->block_size, replica.size());
            nsblock->replica.erase(cs_id);
            return false;
        }
    }
    /// Then block_version >= 0
    if (block_version < nsblock->version) {
        nsblock->replica.erase(cs_id);
        LOG(INFO, "Block #%ld C%d has old version V%ld %ld now: V%ld %ld replica= %lu",
            block_id, cs_id, block_version, block_size,
            nsblock->version, nsblock->block_size, nsblock->replica.size());
        if (nsblock->replica.empty() && !safe_mode) {
            LOG(WARNING, "Data lost #%ld C%d V%ld %ld -> V%ld %ld",
                block_id, cs_id, block_version, block_size,
                nsblock->version, nsblock->block_size);
            nsblock->version = block_version;
            nsblock->block_size = block_size;
        } else {
            LOG(DEBUG, "Drop replica #%ld C%d V%ld try recover R%lu,",
                block_id, cs_id, block_version , nsblock->replica.size());
            TryRecover(nsblock);
            return false;
        }
    } else if (block_version > nsblock->version) {
        LOG(INFO, "Block #%ld update by C%d from V%ld %ld to V%ld %ld",
            block_id, cs_id, nsblock->version, nsblock->block_size,
            block_version, block_size);
        nsblock->version = block_version;
        nsblock->block_size = block_size;
    } // else block_version == nsblock->version, normal block report

    if (replica.insert(cs_id).second) {
        LOG(INFO, "New replica C%d V%ld %ld for #%ld R%lu",
            cs_id, block_version, block_size, block_id, replica.size());
    }
    TryRecover(nsblock);
    return true;
}

bool BlockMapping::UpdateIncompleteBlock(NSBlock* nsblock,
                                         int32_t cs_id, int64_t block_size,
                                         int64_t block_version, bool safe_mode) {
    int64_t block_id = nsblock->id;
    std::set<int32_t>& inc_replica = nsblock->incomplete_replica;
    std::set<int32_t>& replica = nsblock->replica;
    if (block_version < 0) {
        inc_replica.insert(cs_id);
        return true;
    }
    /// Then block_version >= 0
    if (block_version < nsblock->version) {
        nsblock->replica.erase(cs_id);
        LOG(INFO, "Block #%ld C%d has old version V%ld %ld now: V%ld %ld replica= %lu",
            block_id, cs_id, block_version, block_size,
            nsblock->version, nsblock->block_size, nsblock->replica.size());
        if (nsblock->replica.empty() && !safe_mode) {
            LOG(WARNING, "Data lost #%ld C%d V%ld %ld -> V%ld %ld",
                block_id, cs_id, block_version, block_size,
                nsblock->version, nsblock->block_size);
            nsblock->version = block_version;
            nsblock->block_size = block_size;
        } else if (nsblock->replica.size() + nsblock->incomplete_replica.size() >= 2) {
            LOG(DEBUG, "Drop replica #%ld C%d V%ld R%lu IR%lu",
                block_id, cs_id, block_version , nsblock->replica.size(), inc_replica.size());
            return false;
        } else {
            if (!safe_mode
                && nsblock->recover_stat != kIncomplete) {
                LOG(DEBUG, "UpdateBlock #%ld by C%d rep_num %lu, add to recover",
                    block_id, cs_id, nsblock->replica.size());
                TryRecover(nsblock);
            }
            return true;
        }
    }

    if (replica.insert(cs_id).second) {
        LOG(INFO, "Incomplete block replica finish %ld C%d V%ld %ld R%lu IR%lu",
            block_id, cs_id, block_version, block_size, replica.size(), inc_replica.size());
    }
    TryRecover(nsblock);
    return true;
}
        
bool BlockMapping::UpdateBlockInfo(int64_t id, int32_t server_id, int64_t block_size,
                                   int64_t block_version, bool safe_mode) {
    MutexLock lock(&mu_);
    NSBlockMap::iterator it = block_map_.find(id);
    if (it == block_map_.end()) { //have been removed
        LOG(DEBUG, "UpdateBlockInfo C%d #%ld has been removed", server_id, id);
        return false;
    }
    NSBlock* nsblock = it->second;
    switch (nsblock->recover_stat) {
      case kBlockWriting:
        return UpdateWritingBlock(nsblock, server_id, block_size, block_version, safe_mode);
      case kIncomplete:
        return UpdateIncompleteBlock(nsblock, server_id, block_size, block_version, safe_mode);
      default:
        return UpdateNormalBlock(nsblock, server_id, block_size, block_version, safe_mode);
    }
    /*
    if (block_version < 0) {
        if (nsblock->version >= 0) {
            if (nsblock->recover_stat == kBlockWriting || nsblock->recover_stat == kCheck) {
                return true;
            } else {
                LOG(WARNING, "Block wrong stat %s #%ld C%d V%ld %ld",
                    RecoverStat_Name(nsblock->recover_stat).c_str(),
                    id, server_id, block_version, block_size);
                abort();
            }
        } // else writing block
    } else {
        bool inc_ret = nsblock->incomplete_replica.erase(server_id);
        if (inc_ret) {
            if (nsblock->recover_stat == kIncomplete) {
                LOG(INFO, "Closed incomplete block #%ld at C%d V%ld",
                    id, server_id, block_version);
                RemoveFromIncomplete(nsblock, server_id, id);
            }
        }
        if (!safe_mode && nsblock->recover_stat == kBlockWriting
            && nsblock->incomplete_replica.empty()) {   /// Writing -> Normal
            SetState(nsblock, kNotInRecover);
        }
        if (block_version > nsblock->version) { // Block received
            LOG(INFO, "Block #%ld update by C%d from V%ld %ld to V%ld %ld",
                id, server_id, nsblock->version, nsblock->block_size,
                block_version, block_size);
            nsblock->version = block_version;
            nsblock->block_size = block_size;
        } else if (block_version == nsblock->version) { // another received block
            if (block_size != nsblock->block_size) {
                LOG(WARNING, "Block #%ld V%ld size mismatch, old: %ld new: %ld",
                    id, block_version, nsblock->block_size, block_size);
                if (!FLAGS_bfs_bug_tolerant) abort();
                nsblock->replica.erase(server_id);
                return false;
            }
        } else if (block_version < nsblock->version) {
            nsblock->replica.erase(server_id);
            LOG(INFO, "Block #%ld C%d has old version V%ld %ld now: V%ld %ld replica= %lu",
                id, server_id, block_version, block_size,
                nsblock->version, nsblock->block_size, nsblock->replica.size());
            if (nsblock->recover_stat == kBlockWriting) {
                if (nsblock->incomplete_replica.size()) {
                    SetState(nsblock, kIncomplete);
                }
            }
            if (nsblock->replica.empty() && !safe_mode) {
                LOG(WARNING, "Data lost #%ld C%d V%ld %ld -> V%ld %ld",
                    id, server_id, block_version, block_size,
                    nsblock->version, nsblock->block_size);
                nsblock->version = block_version;
                nsblock->block_size = block_size;
            } else if (nsblock->replica.size() + nsblock->incomplete_replica.size() >= 2) {
                return false;
            } else {
                if (!safe_mode
                    && nsblock->recover_stat != kCheck
                    && nsblock->recover_stat != kIncomplete) {
                    LOG(DEBUG, "UpdateBlock #%ld by C%d rep_num %lu, add to recover",
                        id, server_id, nsblock->replica.size());
                    AddToRecover(nsblock);
                }
                return true;
            }
        }
    }

    if (SetStateIf(nsblock, kLost, kNotInRecover)) lost_blocks_.erase(id);

    std::pair<std::set<int32_t>::iterator, bool> ret = nsblock->replica.insert(server_id);
    int32_t cur_replica_num = nsblock->replica.size();
    int32_t expect_replica_num = nsblock->expect_replica_num;
    bool clean_redundancy = false;
    if (clean_redundancy && cur_replica_num > expect_replica_num) {
        LOG(INFO, "Too much replica #%ld cur=%d expect=%d server=C%d ",
            id, cur_replica_num, expect_replica_num, server_id);
        nsblock->replica.erase(ret.first);
        return false;
    }
    if (ret.second) {
        LOG(INFO, "New replica C%d V%ld %ld for #%ld total: %d",
            server_id, block_version, block_size, id, cur_replica_num);
    }
    if (cur_replica_num < expect_replica_num && nsblock->version >= 0) {
        if (!safe_mode && nsblock->recover_stat != kCheck
            && nsblock->recover_stat != kIncomplete) {
            LOG(DEBUG, "UpdateBlock #%ld by C%d rep_num %d, add to recover",
                id, server_id, cur_replica_num);
            AddToRecover(nsblock);
        }
    }
    return true;
    */
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
        LOG(WARNING, "RemoveBlock #%ld not found", block_id);
        return;
    }
    NSBlock* block = it->second;
    if (block->recover_stat == kIncomplete) {
        for (std::set<int32_t>::iterator it = block->incomplete_replica.begin();
             it != block->incomplete_replica.end(); ++it) {
            int32_t cs_id = *it;
            LOG(DEBUG, "Erase C%d #%ld from incomplete_", cs_id, block_id);
            IncompleteList::iterator c_it = incomplete_.find(cs_id);
            if (c_it != incomplete_.end()) {
                std::set<int64_t>& cs = c_it->second;
                cs.erase(block_id);
                if (cs.empty()) {
                    incomplete_.erase(c_it);
                }
            }
        }
    } else if (block->recover_stat == kLost) {
        lost_blocks_.erase(block_id);
    } else if (block->recover_stat == kHiRecover) {
        hi_pri_recover_.erase(block_id);
    } else if (block->recover_stat == kLoRecover) {
        lo_pri_recover_.erase(block_id);
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
        LOG(INFO, "FinishBlock update version from V%ld to V%ld",
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
        std::set<int32_t>& inc_replica = block->incomplete_replica;
        std::set<int32_t>& replica = block->replica;
        if (inc_replica.erase(cs_id)) {
            incomplete_[cs_id].erase(block_id);
        } else {
            bool ret = replica.erase(block_id);
            assert(ret);
        }
        if (block->recover_stat == kIncomplete) {
            if (inc_replica.empty()) SetState(block, kNotInRecover);
        } else if (block->recover_stat == kBlockWriting) {
            if (inc_replica.size() > 0) {
                SetState(block, kIncomplete);
            } else {
                SetState(block, kNotInRecover);
            }
            LOG(WARNING, "Incomplete block C%d #%ld dead replica= %lu",
                cs_id, block_id, replica.size());
            std::set<int32_t>::iterator cs_it = inc_replica.begin();
            for (; cs_it != inc_replica.end(); ++cs_it) {
                incomplete_[*cs_it].insert(block_id);
                LOG(INFO, "Insert C%d #%ld to incomplete_", *cs_it, block_id);
            }
        }   // else Normal check low hi

        if (block->recover_stat != kIncomplete
            && replica.size() < block->expect_replica_num) {
            LOG(DEBUG, "Dead replica at C%d add #%ld to recover R%lu",
                cs_id, block_id, replica.size());
            TryRecover(block);
        }
    }
}

void BlockMapping::PickRecoverBlocks(int32_t cs_id, int32_t block_num,
                                     std::map<int64_t, int32_t>* recover_blocks) {
    MutexLock lock(&mu_);
    CheckList::iterator check_it =
        recover_check_.insert(std::make_pair(cs_id, std::set<int64_t>())).first;
    int32_t quota = FLAGS_recover_speed - (check_it->second).size();
    LOG(DEBUG, "C%d has %lu pending_recover blocks", cs_id, (check_it->second).size());
    quota = quota < block_num ? quota : block_num;
    LOG(DEBUG, "Before Pick: recover num(hi/lo): %ld/%ld ",
        hi_pri_recover_.size(), lo_pri_recover_.size());
    PickRecoverFromSet(cs_id, quota, &hi_pri_recover_, recover_blocks, &(check_it->second));
    PickRecoverFromSet(cs_id, quota, &lo_pri_recover_, recover_blocks, &(check_it->second));
    LOG(DEBUG, "After Pick: recover num(hi/lo): %ld/%ld ", hi_pri_recover_.size(), lo_pri_recover_.size());
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

void BlockMapping::GetCloseBlocks(int32_t cs_id,
                                  google::protobuf::RepeatedField<int64_t>* close_blocks) {
    MutexLock lock(&mu_);
    IncompleteList::iterator c_it = incomplete_.find(cs_id);
    if (c_it != incomplete_.end()) {
        const std::set<int64_t>& blocks = c_it->second;
        for (std::set<int64_t>::iterator it = blocks.begin(); it != blocks.end(); ++it) {
            LOG(INFO, "GetCloseBlocks #%ld at C%d ", *it, cs_id);
            close_blocks->Add(*it);
        }
    }
}

void BlockMapping::GetStat(int64_t* lo_recover_num, int64_t* pending_num,
                           int64_t* hi_recover_num, int64_t* lost_num,
                           int64_t* incomplete_num) {
    MutexLock lock(&mu_);
    if (lo_recover_num) {
        *lo_recover_num = lo_pri_recover_.size();
    }
    if (pending_num) {
        *pending_num = 0;
        for (CheckList::iterator it = recover_check_.begin(); it != recover_check_.end(); ++it) {
            *pending_num += (it->second).size();
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
        for (IncompleteList::iterator it = incomplete_.begin(); it != incomplete_.end(); ++it) {
            *incomplete_num += (it->second).size();
        }
    }
}
void BlockMapping::ListRecover(std::string* hi_recover, std::string* lo_recover, std::string* lost,
                               std::string* check, std::string* incomplete) {
    MutexLock lock(&mu_);
    for (std::set<int64_t>::iterator it = lo_pri_recover_.begin(); it != lo_pri_recover_.end(); ++it) {
        lo_recover->append(common::NumToString(*it) + " ");
    }

    for (std::set<int64_t>::iterator it = hi_pri_recover_.begin(); it != hi_pri_recover_.end(); ++it) {
        hi_recover->append(common::NumToString(*it) + " ");
    }

    for (std::set<int64_t>::iterator it = lost_blocks_.begin(); it != lost_blocks_.end(); ++it) {
        lost->append(common::NumToString(*it) + " ");
    }

    for (CheckList::iterator it = recover_check_.begin(); it != recover_check_.end(); ++it) {
        check->append(common::NumToString(it->first) + ": ");
        const std::set<int64_t>& block_set = it->second;
        for (std::set<int64_t>::iterator block_it = block_set.begin(); block_it != block_set.end();
             ++block_it) {
            check->append(common::NumToString(*block_it) + " ");
        }
        check->append("<br>");
    }

    for (CheckList::iterator it = incomplete_.begin(); it != incomplete_.end(); ++it) {
        incomplete->append(common::NumToString(it->first) + ": ");
        const std::set<int64_t>& block_set = it->second;
        for (std::set<int64_t>::iterator block_it = block_set.begin(); block_it != block_set.end();
             ++block_it) {
            incomplete->append(common::NumToString(*block_it) + " ");
        }
        incomplete->append("<br>");
    }
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
            LOG(DEBUG, "All Replica lost #%ld , give up recover.", block_id);
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
        assert(cur_block->recover_stat == kHiRecover || cur_block->recover_stat == kLoRecover);
        cur_block->recover_stat = kCheck;
        LOG(INFO, "PickRecoverBlocks for C%d #%ld source: C%d ", cs_id, block_id, src_id);
        thread_pool_.DelayTask(FLAGS_recover_timeout * 1000,
            boost::bind(&BlockMapping::CheckRecover, this, cs_id, block_id));
        recover_set->erase(it++);
    }
}

void BlockMapping::TryRecover(NSBlock* block) {
    mu_.AssertHeld();
    int64_t block_id = block->id;
    if (block->recover_stat == kLoRecover) {
        lo_pri_recover_.erase(block_id);
    } else if (block->recover_stat == kHiRecover) {
        hi_pri_recover_.erase(block_id);
    }
    if (block->replica.size() < block->expect_replica_num) {
        if (block->replica.size() == 0) {
            lost_blocks_.insert(block_id);
            block->recover_stat = kLost;
            LOG(INFO, "[TryRecover] lost block #%ld ", block_id);
        } else if (block->replica.size() == 1) {
            hi_pri_recover_.insert(block_id);
            LOG(INFO, "[TryRecover] need more recover: #%ld %s->kHiRecover",
                RecoverStat_Name(block->recover_stat).c_str(), block_id);
            block->recover_stat = kHiRecover;
        } else {
            lo_pri_recover_.insert(block_id);
            LOG(INFO, "[TryRecover] need more recover: #%ld %s->kLoRecover",
                RecoverStat_Name(block->recover_stat).c_str(), block_id);
            block->recover_stat = kLoRecover;
        }
    } else if (block->recover_stat != kNotInRecover) {
        SetState(block, kNotInRecover);
        LOG(INFO, "Recover done #%ld C%ld %ld R%lu",
            block_id, block->version, block->block_size, block->replica.size());
    }
}

void BlockMapping::CheckRecover(int32_t cs_id, int64_t block_id) {
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

void BlockMapping::RemoveFromIncomplete(NSBlock* block, int32_t cs_id, int64_t block_id) {
    mu_.AssertHeld();
    if (block->incomplete_replica.size() == 0) {
        SetStateIf(block, kIncomplete, kNotInRecover);
    }
    IncompleteList::iterator incomplete_it = incomplete_.find(cs_id);
    if (incomplete_it != incomplete_.end()) {
        std::set<int64_t>& incomplete_set = incomplete_it->second;
        incomplete_set.erase(block_id);
        if (incomplete_set.empty()) {
            incomplete_.erase(incomplete_it);
        }
    } else {
        LOG(WARNING, "RemoveFromIncomplete not find, C%d #%ld", cs_id, block_id);
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
    LOG(INFO, "SetStateIf #%ld %s->%s",
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
