// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "block_mapping_manager.h"

#include "proto/status_code.pb.h"

#include <common/logging.h>
#include <common/string_util.h>
#include <common/mutex.h>

DECLARE_int32(web_recover_list_size);

namespace baidu {
namespace bfs {

BlockMappingManager::BlockMappingManager(int32_t bucket_num) :
    blockmapping_bucket_num_(bucket_num) {
    block_mapping_.resize(blockmapping_bucket_num_);
    for (size_t i = 0; i < block_mapping_.size(); i++) {
        block_mapping_[i] = new BlockMapping();
    }
}

BlockMappingManager::~BlockMappingManager() {
}

int32_t BlockMappingManager::GetBucketOffset(int64_t block_id) {
    return block_id % blockmapping_bucket_num_;
}

bool BlockMappingManager::GetBlock(int64_t block_id, NSBlock* block) {
    int32_t bucket_offset = GetBucketOffset(block_id);
    return block_mapping_[bucket_offset]->GetBlock(block_id, block);
}

bool BlockMappingManager::GetLocatedBlock(int64_t block_id, std::vector<int32_t>* replica, int64_t* block_size) {
    int32_t bucket_offset = GetBucketOffset(block_id);
    return block_mapping_[bucket_offset]->GetLocatedBlock(block_id, replica, block_size);
}

bool BlockMappingManager::ChangeReplicaNum(int64_t block_id, int32_t replica_num) {
    int32_t bucket_offset = GetBucketOffset(block_id);
    return block_mapping_[bucket_offset]->ChangeReplicaNum(block_id, replica_num);
}

void BlockMappingManager::AddNewBlock(int64_t block_id, int32_t replica,
                 int64_t version, int64_t block_size,
                 const std::vector<int32_t>* init_replicas) {
    int32_t bucket_offset = GetBucketOffset(block_id);
    block_mapping_[bucket_offset]->AddNewBlock(block_id, replica, version, block_size, init_replicas);
}

bool BlockMappingManager::UpdateBlockInfo(int64_t block_id, int32_t server_id, int64_t block_size,
                     int64_t block_version) {
    int32_t bucket_offset = GetBucketOffset(block_id);
    bool ret = block_mapping_[bucket_offset]->UpdateBlockInfo(block_id, server_id,
                                                              block_size, block_version);
    return ret;
}

void BlockMappingManager::RemoveBlocksForFile(const FileInfo& file_info) {
    for (int i = 0; i < file_info.blocks_size(); i++) {
        int32_t bucket_offset = GetBucketOffset(file_info.blocks(i));
        block_mapping_[bucket_offset]->RemoveBlocksForFile(file_info);
    }
}

void BlockMappingManager::RemoveBlock(int64_t block_id) {
    int32_t bucket_offset = GetBucketOffset(block_id);
    block_mapping_[bucket_offset]->RemoveBlock(block_id);
}

void BlockMappingManager::DealWithDeadNode(int32_t cs_id, const std::set<int64_t>& blocks) {
    std::vector<std::set<int64_t> > blocks_array;
    blocks_array.resize(block_mapping_.size());
    for (std::set<int64_t>::iterator it = blocks.begin(); it != blocks.end(); ++it) {
        int32_t bucket_offset = GetBucketOffset(*it);
        blocks_array[bucket_offset].insert(*it);
    }
    for (size_t i = 0; i < blocks_array.size(); i++) {
        block_mapping_[i]->DealWithDeadNode(cs_id, blocks_array[i]);
    }
}

StatusCode BlockMappingManager::CheckBlockVersion(int64_t block_id, int64_t version) {
    int32_t bucket_offset = GetBucketOffset(block_id);
    return block_mapping_[bucket_offset]->CheckBlockVersion(block_id, version);
}

void BlockMappingManager::PickRecoverBlocks(int32_t cs_id, int32_t block_num,
                       std::vector<std::pair<int64_t, std::set<int32_t> > >* recover_blocks,
                       int32_t* hi_num) {
    int cur_check_num = 0;
    for (int i = 0; i < blockmapping_bucket_num_; i++) {
        RecoverBlockNum num;
        block_mapping_[i]->GetStat(cs_id, &num);
        cur_check_num += (num.lo_pending + num.hi_pending);
    }
    block_num -= cur_check_num;
    for (int i = 0; i < blockmapping_bucket_num_ && (size_t)block_num > recover_blocks->size(); i++) {
        block_mapping_[i]->PickRecoverBlocks(cs_id, block_num - recover_blocks->size(), recover_blocks, kHigh);
    }
    for (int i = 0; i < blockmapping_bucket_num_ && (size_t)block_num > recover_blocks->size(); i++) {
        block_mapping_[i]->PickRecoverBlocks(cs_id, block_num - recover_blocks->size(), recover_blocks, kPreHigh);
    }
    *(hi_num) += recover_blocks->size();
    for (int i = 0; i < blockmapping_bucket_num_ && (size_t)block_num > recover_blocks->size(); i++) {
        block_mapping_[i]->PickRecoverBlocks(cs_id, block_num - recover_blocks->size(), recover_blocks, kLow);
    }
    for (int i = 0; i < blockmapping_bucket_num_ && (size_t)block_num > recover_blocks->size(); i++) {
        block_mapping_[i]->PickRecoverBlocks(cs_id, block_num - recover_blocks->size(), recover_blocks, kPreLow);
    }
}

void BlockMappingManager::ProcessRecoveredBlock(int32_t cs_id, int64_t block_id) {
    int32_t bucket_offset = GetBucketOffset(block_id);
    block_mapping_[bucket_offset]->ProcessRecoveredBlock(cs_id, block_id);
}

void BlockMappingManager::GetCloseBlocks(int32_t cs_id, google::protobuf::RepeatedField<int64_t>* close_blocks) {
    for (size_t i = 0; i < block_mapping_.size(); i++) {
        block_mapping_[i]->GetCloseBlocks(cs_id, close_blocks);
    }
}

void BlockMappingManager::GetStat(int32_t cs_id, RecoverBlockNum* recover_num) {
    for (size_t i = 0; i < block_mapping_.size(); i++) {
        RecoverBlockNum cur_num;
        block_mapping_[i]->GetStat(cs_id, &cur_num);
        recover_num->lo_recover_num += cur_num.lo_recover_num;
        recover_num->hi_recover_num += cur_num.hi_recover_num;
        recover_num->lo_pre_recover_num += cur_num.lo_pre_recover_num;
        recover_num->hi_pre_recover_num += cur_num.hi_pre_recover_num;
        recover_num->lo_pending += cur_num.lo_pending;
        recover_num->hi_pending += cur_num.hi_pending;
        recover_num->lo_pre_pending += cur_num.lo_pre_pending;
        recover_num->hi_pre_pending += cur_num.hi_pre_pending;
        recover_num->lost_num += cur_num.lost_num;
        recover_num->incomplete_num += cur_num.incomplete_num;
    }
}

void BlockMappingManager::ListRecover(RecoverBlockSet* recover_blocks) {
    for (size_t i = 0; i < block_mapping_.size(); i++) {
        block_mapping_[i]->ListRecover(recover_blocks, FLAGS_web_recover_list_size);
    }
}

void BlockMappingManager::SetSafeMode(bool safe_mode) {
    for (size_t i = 0; i < block_mapping_.size(); i++) {
        block_mapping_[i]->SetSafeMode(safe_mode);
    }
}

void BlockMappingManager::MarkIncomplete(int64_t block_id) {
    int32_t bucket_offset = GetBucketOffset(block_id);
    block_mapping_[bucket_offset]->MarkIncomplete(block_id);
}

void BlockMappingManager::MoveReplicasToReadonlySet(int32_t cs_id, const std::set<int64_t>& blocks) {
    for(std::set<int64_t>::const_iterator it = blocks.begin(); it != blocks.end(); ++it) {
        MutexLock lock(&mu_);
        int32_t bucket_offset = GetBucketOffset(*it);
        block_mapping_[bucket_offset]->MoveReplicaToReadonlySet(cs_id, *it);
    }
}

size_t BlockMappingManager::GetHiPreRecoverSetSize() {
    MutexLock lock(&mu_);
    RecoverBlockNum recover_block_num;
    for (int i = 0; i < blockmapping_bucket_num_; i++) {
        RecoverBlockNum cur_num;
        block_mapping_[i]->GetStat(-1, &cur_num);
        recover_block_num.hi_pre_recover_num += cur_num.hi_pre_recover_num;
        recover_block_num.hi_pre_pending += cur_num.hi_pre_pending;
    }
    return recover_block_num.hi_pre_recover_num +
            recover_block_num.hi_pre_pending;
}

} //namespace bfs
} //namespace baidu
