// Copyright (c) 2014, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "block_mapping.h"

#include <vector>
#include <boost/bind.hpp>
#include <gflags/gflags.h>

#include <common/counter.h>
#include <common/logging.h>
#include <common/string_util.h>

DECLARE_int32(recover_speed);
DECLARE_int32(hi_recover_timeout);
DECLARE_int32(lo_recover_timeout);
DECLARE_bool(bfs_bug_tolerant);
DECLARE_bool(clean_redundancy);
DECLARE_int32(web_recover_list_size);

namespace baidu {
namespace bfs {

extern common::Counter g_blocks_num;

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

BlockMapping::BlockMapping(ThreadPool* thread_pool) : thread_pool_(thread_pool) {}

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

bool BlockMapping::GetLocatedBlock(int64_t id, std::vector<int32_t>* replica, int64_t* size, RecoverStat* status) {
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
    if (replica->empty()) {
        LOG(DEBUG, "Block #%ld lost all replica", id);
    }
    *size = block->block_size;
    *status = block->recover_stat;
    return true;
}

bool BlockMapping::ChangeReplicaNum(int64_t block_id, int32_t replica_num) {
    MutexLock lock(&mu_);
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
        if (size) {
            nsblock->recover_stat = kLost;
            lost_blocks_.insert(block_id);
        } else {
            nsblock->recover_stat = kBlockWriting;
        }

        if (version < 0) {
            LOG(INFO, "Rebuild writing block #%ld V%ld %ld", block_id, version, size);
        } else {
            LOG(DEBUG, "Rebuild block #%ld V%ld %ld", block_id, version, size);
        }
    }
    g_blocks_num.Inc();
    MutexLock lock(&mu_);
    common::timer::TimeChecker insert_time;
    std::pair<NSBlockMap::iterator, bool> ret =
        block_map_.insert(std::make_pair(block_id, nsblock));
    assert(ret.second == true);
    insert_time.Check(10 * 1000, "[AddNewBlock] InsertToBlockMapping");
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
                    LOG(INFO, "Drop replica #%ld C%d V%ld R%lu IR%lu",
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
    if (inc_replica.empty() && replica.size() >= nsblock->expect_replica_num) {
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
            if (replica.size() >= nsblock->expect_replica_num) {
                LOG(INFO, "Drop incomplete block #%ld from C%d, now V%ld %ld R%lu",
                    block_id, cs_id, nsblock->version, nsblock->block_size, replica.size());
                return false;
            } else {
                LOG(INFO, "Keep incomplete block #%ld from C%d, now V%ld %ld R%lu IR%lu",
                    block_id, cs_id, nsblock->version, nsblock->block_size,
                    replica.size(), inc_replica.size());
                return true;
            }
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

    TryRecover(nsblock);
    if (FLAGS_clean_redundancy && replica.size() > nsblock->expect_replica_num) {
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
                    LOG(INFO, "Drop replica #%ld C%d V%ld R%lu IR%lu",
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
    if (inc_replica.empty()) {
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
    common::timer::TimeChecker update_block_timer;
    NSBlock* block = NULL;
    if (!GetBlockPtr(block_id, &block)) {
        LOG(DEBUG, "UpdateBlockInfo C%d #%ld has been removed", server_id, block_id);
        return false;
    }
    update_block_timer.Check(10 * 1000, "[UpdateBlockInfo] GetBlockPtr");
    bool ret = true;;
    switch (block->recover_stat) {
      case kBlockWriting:
        ret = UpdateWritingBlock(block, server_id, block_size, block_version);
        update_block_timer.Check(10 * 1000, "[UpdateBlockInfo] UpdateWritingBlock");
        return ret;
      case kIncomplete:
        ret = UpdateIncompleteBlock(block, server_id, block_size, block_version);
        update_block_timer.Check(10 * 1000, "[UpdateBlockInfo] UpdateIncomleteBlock");
        return ret;
      case kLost:
        if (block->version < 0) {
            bool ret = UpdateWritingBlock(block, server_id, block_size, block_version);
            if (block->recover_stat == kLost) {
                lost_blocks_.erase(block_id);
                if (block->version < 0) {
                    block->recover_stat = kBlockWriting;
                } else {
                    LOG(WARNING, "Update lost block #%ld V%ld ", block_id, block->version);
                }
            }
            return ret;
        } else {
            ret = UpdateNormalBlock(block, server_id, block_size, block_version);
            update_block_timer.Check(10 * 1000, "[UpdateBlockInfo] UpdateNormalBlock");
            return ret;
        }
      default:  // kNotInRecover kLow kHi kLost kCheck
        ret = UpdateNormalBlock(block, server_id, block_size, block_version);
        update_block_timer.Check(10 * 1000, "[UpdateBlockInfo] UpdateNormalBlock");
        return ret;
    }
}

void BlockMapping::RemoveBlocksForFile(const FileInfo& file_info,
                                       std::map<int64_t, std::set<int32_t> >* blocks) {
    for (int i = 0; i < file_info.blocks_size(); i++) {
        int64_t block_id = file_info.blocks(i);
        RemoveBlock(block_id, blocks);
        LOG(INFO, "Remove block #%ld for %s", block_id, file_info.name().c_str());
    }
}

void BlockMapping::RemoveBlock(int64_t block_id, std::map<int64_t, std::set<int32_t> >* blocks) {
    MutexLock lock(&mu_);
    NSBlock* block = NULL;
    if (!GetBlockPtr(block_id, &block)) {
        LOG(WARNING, "RemoveBlock #%ld not found", block_id);
        return;
    }
    if (blocks) {
        std::set<int32_t>& block_cs = (*blocks)[block_id];
        block_cs.insert(block->incomplete_replica.begin(), block->incomplete_replica.end());
        block_cs.insert(block->replica.begin(), block->replica.end());
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
    g_blocks_num.Dec();
}

StatusCode BlockMapping::CheckBlockVersion(int64_t block_id, int64_t version) {
    MutexLock lock(&mu_);
    NSBlock* block = NULL;
    if (!GetBlockPtr(block_id, &block)) {
        LOG(WARNING, "CheckBlockVersion can not find block: #%ld ", block_id);
        return kNsNotFound;
    }
    if (block->version != version) {
        LOG(INFO, "CheckBlockVersion fail #%ld V%ld to V%ld",
            block_id, block->version, version);
        return kVersionError;
    }
    return kOK;
}

void BlockMapping::DealWithDeadBlockInternal(int32_t cs_id, int64_t block_id) {
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
        if (inc_replica.empty()) {
            SetState(block, kNotInRecover);
        }
    } else if (block->recover_stat == kBlockWriting) {
        LOG(INFO, "Writing block C%d #%ld dead R%lu IR%lu",
            cs_id, block_id, replica.size(), inc_replica.size());
        if (inc_replica.size() > 0) {
            SetState(block, kIncomplete);
            InsertToIncomplete(block_id, inc_replica);
        } else {
            SetState(block, kNotInRecover);
        }
    }   // else Normal check low hi

    LOG(DEBUG, "Dead replica at C%d add #%ld R%lu try recover",
        cs_id, block_id, replica.size());
    TryRecover(block);
}

void BlockMapping::DealWithDeadNode(int32_t cs_id, const std::set<int64_t>& blocks) {
    for (std::set<int64_t>::iterator it = blocks.begin(); it != blocks.end(); ++it) {
        MutexLock lock(&mu_);
        DealWithDeadBlockInternal(cs_id, *it);
    }
    MutexLock lock(&mu_);
    NSBlock* block = NULL;
    for (std::set<int64_t>::iterator it = hi_recover_check_[cs_id].begin();
            it != hi_recover_check_[cs_id].end(); ++it) {
        if (!GetBlockPtr(*it, &block)) {
            LOG(DEBUG, "DealWithDeadBlocks for C%d can't find block: #%ld ", cs_id, *it);
        } else {
            block->recover_stat = kNotInRecover;
        }
    }
    hi_recover_check_.erase(cs_id);
    for (std::set<int64_t>::iterator it = lo_recover_check_[cs_id].begin();
            it != lo_recover_check_[cs_id].end(); ++it) {
        if (!GetBlockPtr(*it, &block)) {
            LOG(DEBUG, "DealWithDeadBlocks for C%d can't find block: #%ld ", cs_id, *it);
        } else {
            block->recover_stat = kNotInRecover;
        }
    }
    lo_recover_check_.erase(cs_id);
}

void BlockMapping::DealWithDeadBlock(int32_t cs_id, int64_t block_id) {
    MutexLock lock(&mu_);
    DealWithDeadBlockInternal(cs_id, block_id);
}

void BlockMapping::PickRecoverBlocks(int32_t cs_id, int32_t block_num,
                                     std::vector<std::pair<int64_t, std::set<int32_t> > >* recover_blocks,
                                     RecoverPri pri) {
    MutexLock lock(&mu_);
    if ((pri == kHigh && hi_pri_recover_.empty()) || (pri == kLow && lo_pri_recover_.empty())) {
        return;
    }
    std::set<int64_t>* target_set = pri == kHigh ? &hi_pri_recover_ : &lo_pri_recover_;
    std::set<int64_t>* check_set =
        pri == kHigh ? &hi_recover_check_[cs_id] : &lo_recover_check_[cs_id];
    LOG(DEBUG, "Before Pick: C%d has %lu pending_recover blocks quota=%d pri=%s",
            cs_id, check_set->size(), block_num, RecoverPri_Name(pri).c_str());

    common::timer::TimeChecker pick_timer;
    std::set<int64_t>::iterator it = target_set->begin();
    // leave 3 seconds buffer
    int32_t timeout = 3 + (pri == kHigh ? FLAGS_hi_recover_timeout : FLAGS_lo_recover_timeout);
    while (static_cast<int>(recover_blocks->size()) < block_num && it != target_set->end()) {
        NSBlock* cur_block = NULL;
        if (!GetBlockPtr(*it, &cur_block)) { // block is removed
            LOG(DEBUG, "PickRecoverBlocks for C%d can't find block: #%ld ", cs_id, *it);
            target_set->erase(it++);
            continue;
        }
        const std::set<int32_t>& replica = cur_block->replica;
        int64_t block_id = cur_block->id;
        if (replica.size() >= cur_block->expect_replica_num) {
            LOG(DEBUG, "Replica num enough #%ld %lu", block_id, replica.size());
            target_set->erase(it++);
            SetState(cur_block, kNotInRecover);
            continue;
        }
        if (replica.size() == 0) {
            LOG(WARNING, "All Replica lost #%ld , give up recover.", block_id);
            abort();
            SetStateIf(cur_block, kAny, kLost);
            lost_blocks_.insert(block_id);
            target_set->erase(it++);
            continue;
        }
        if (replica.find(cs_id) == replica.end()) {
            ++it;
            continue;
        }
        recover_blocks->push_back(std::make_pair(block_id, replica));
        check_set->insert(block_id);
        assert(cur_block->recover_stat == kHiRecover || cur_block->recover_stat == kLoRecover);
        cur_block->recover_stat = kCheck;
        LOG(INFO, "PickRecoverBlocks for C%d #%ld %s",
                cs_id, block_id, RecoverStat_Name(cur_block->recover_stat).c_str());
        thread_pool_->DelayTask(timeout * 1000,
            boost::bind(&BlockMapping::CheckRecover, this, cs_id, block_id));
        target_set->erase(it++);
    }
    pick_timer.Check(100 * 1000, "[PickRecoverBlocks] pick recover");
    LOG(DEBUG, "After Pick: C%d has %u pending_recover blocks pri=%s",
            cs_id, check_set->size(), RecoverPri_Name(pri).c_str());
    LOG(INFO, "C%d picked %lu blocks to recover", cs_id, recover_blocks->size());
}

bool BlockMapping::RemoveFromRecoverCheckList(int32_t cs_id, int64_t block_id) {
    mu_.AssertHeld();
    std::set<int64_t>::iterator it = lo_recover_check_[cs_id].find(block_id);
    if (it != lo_recover_check_[cs_id].end()){
        lo_recover_check_[cs_id].erase(it);
        LOG(DEBUG, "Remove #%ld from lo_recover_check", block_id);
        if (lo_recover_check_[cs_id].empty()) {
            lo_recover_check_.erase(cs_id);
        }
    } else {
        it = hi_recover_check_[cs_id].find(block_id);
        if (it != hi_recover_check_[cs_id].end()) {
            LOG(DEBUG, "Remove #%ld from hi_recover_check", block_id);
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
void BlockMapping::ProcessRecoveredBlock(int32_t cs_id, int64_t block_id, StatusCode status) {
    MutexLock lock(&mu_);
    bool ret = RemoveFromRecoverCheckList(cs_id, block_id);
    NSBlock* block = NULL;
    if (!GetBlockPtr(block_id, &block)) {
        LOG(DEBUG, "ProcessRecoveredBlock for C%d can't find block: #%ld ", cs_id, block_id);
        return;
    }
    if (status == kCsNotFound) {
        LOG(WARNING, "C%d doesnt't have block #%ld, remove from block mapping", cs_id, block_id);
        block->replica.erase(cs_id);
    }
    if (ret) {
        SetState(block, kNotInRecover);
    } else {
        LOG(WARNING, "RemoveFromRecoverCheckList fail #%ld C%d %s",
            block_id, cs_id, RecoverStat_Name(block->recover_stat).c_str());
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

void BlockMapping::GetStat(int32_t cs_id, RecoverBlockNum* recover_num) {
    MutexLock lock(&mu_);
    recover_num->lo_recover_num = lo_pri_recover_.size();
    if (cs_id != -1) {
        recover_num->lo_pending += lo_recover_check_[cs_id].size();
    } else {
        for (CheckList::iterator it = lo_recover_check_.begin(); it != lo_recover_check_.end(); ++it) {
            recover_num->lo_pending += (it->second).size();
        }
    }
    if (cs_id != -1) {
        recover_num->hi_pending += hi_recover_check_[cs_id].size();
    } else {
        for (CheckList::iterator it = hi_recover_check_.begin(); it != hi_recover_check_.end(); ++it) {
            recover_num->hi_pending += (it->second).size();
        }
    }
    recover_num->hi_recover_num = hi_pri_recover_.size();
    recover_num->lost_num = lost_blocks_.size();
    if (cs_id != -1) {
        recover_num->incomplete_num += incomplete_[cs_id].size();
    } else {
        for (CheckList::iterator it = incomplete_.begin(); it != incomplete_.end(); ++it) {
            recover_num->incomplete_num += (it->second).size();
        }
    }
}

void BlockMapping::ListCheckList(const CheckList& check_list, std::map<int32_t, std::set<int64_t> >* result) {
    for (CheckList::const_iterator it = check_list.begin(); it != check_list.end(); ++it) {
        const std::set<int64_t>& block_set = it->second;
        for (std::set<int64_t>::iterator block_it = block_set.begin();
                block_it != block_set.end(); ++block_it) {
            (*result)[it->first].insert(*block_it);
        }
    }
}

void BlockMapping::ListRecoverList(const std::set<int64_t>& recover_set, std::set<int64_t>* result) {
    int half = FLAGS_web_recover_list_size / 2;
    std::set<int64_t>::iterator it = recover_set.begin();
    int count = 0;
    for (; it != recover_set.end() && count != half; ++it) {
        result->insert(*it);
        ++count;
    }
    if (it != recover_set.end()) {
        for (std::set<int64_t>::reverse_iterator rit = recover_set.rbegin();
                rit != recover_set.rend() && count != FLAGS_web_recover_list_size; ++rit) {
            result->insert(*rit);
            ++count;
        }
    }
}

void BlockMapping::ListRecover(RecoverBlockSet* recover_blocks) {
    MutexLock lock(&mu_);
    ListRecoverList(lo_pri_recover_, &(recover_blocks->lo_recover));
    ListRecoverList(hi_pri_recover_, &(recover_blocks->hi_recover));
    ListRecoverList(lost_blocks_, &(recover_blocks->lost));

    ListCheckList(hi_recover_check_, &(recover_blocks->hi_check));
    ListCheckList(lo_recover_check_, &(recover_blocks->lo_check));
    ListCheckList(incomplete_, &(recover_blocks->incomplete));
}

void BlockMapping::TryRecover(NSBlock* block) {
    mu_.AssertHeld();
    if (block->recover_stat == kCheck
        || block->recover_stat == kIncomplete
        || block->recover_stat == kBlockWriting) {
        return;
    }
    int64_t block_id = block->id;
    if (block->replica.size() < block->expect_replica_num) {
        if (block->replica.size() == 0) {
            if (block->block_size && block->recover_stat != kLost) {
                LOG(INFO, "[TryRecover] lost block #%ld ", block_id);
                lost_blocks_.insert(block_id);
                SetState(block, kLost);
                lo_pri_recover_.erase(block_id);
                hi_pri_recover_.erase(block_id);
            } else if (block->block_size == 0 && block->recover_stat == kLost) {
                lost_blocks_.erase(block_id);
                LOG(WARNING, "[TryRecover] empty block #%ld remove from lost", block_id);
            }
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
    } else {
        LOG(INFO, "RemoveFromIncomplete C%d #%ld ", cs_id, block_id);
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

void BlockMapping::MarkIncomplete(int64_t block_id) {
    MutexLock lock(&mu_);
    NSBlock* block = NULL;
    if (!GetBlockPtr(block_id, &block)) {
        return;
    }
    // maybe cs is down and block have been marked with incomplete
    if (block->recover_stat == kBlockWriting) {
        for (std::set<int32_t>::iterator it = block->incomplete_replica.begin();
                it != block->incomplete_replica.end(); ++it) {
            incomplete_[*it].insert(block_id);
            LOG(INFO, "Mark #%ld in C%d incomplete", block_id, *it);
        }
        SetState(block, kIncomplete);
    }
}

} // namespace bfs
} // namespace baidu
