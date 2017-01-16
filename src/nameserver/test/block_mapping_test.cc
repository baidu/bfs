// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//

#define private public
#include "nameserver/block_mapping.h"
#include "proto/status_code.pb.h"

#include <gtest/gtest.h>

namespace baidu {
namespace bfs {

ThreadPool thread_pool;
class BlockMappingTest : public ::testing::Test {
public:
    BlockMappingTest() {}
protected:
};

void AddBlockHelper(BlockMapping* bm, int64_t start_id, int64_t end_id) {
    std::vector<int32_t> replica;
    replica.resize(3);
    for (int64_t id = start_id; id <= end_id; ++id) {
        replica[0] = id % 100;
        replica[1] = (id + 1) % 100;
        replica[2] = (id + 2) % 100;
        bm->AddBlock(id, 3, replica);
    }
}

void RemoveBlockHelper(BlockMapping* bm, int64_t start_id, int64_t end_id) {
    for (int64_t id = start_id; id <= end_id; ++id) {
        bm->RemoveBlock(id, NULL);
    }
}

TEST_F(BlockMappingTest, Basic) {
    BlockMapping* bm = new BlockMapping(&thread_pool);
    // add block 0 - 19 into map
    AddBlockHelper(bm, 0, 19);
    ASSERT_TRUE(bm->block_map_.size() == 20);
    for (int64_t id = 0; id <= 19; ++id) {
        NSBlock block;
        ASSERT_TRUE(bm->GetBlock(id, &block));
    }

    // remove some from map
    RemoveBlockHelper(bm, 0, 9);
    for (int64_t id = 0; id <= 9; ++id) {
        NSBlock block;
        ASSERT_FALSE(bm->GetBlock(id, &block));
    }
    for (int64_t id = 10; id <= 19; ++id) {
        NSBlock block;
        ASSERT_TRUE(bm->GetBlock(id, &block));
    }
}

TEST_F(BlockMappingTest, DoNotRemoteHigherVersionBlock) {
    int64_t block_id = 1;
    int64_t block_version = 2;
    int64_t block_size = 3;
    int32_t replica = 3;
    BlockMapping* bm = new BlockMapping(&thread_pool);
    bm->RebuildBlock(block_id, replica, block_version, block_size);
    BlockMapping::NSBlockMap::iterator it = bm->block_map_.find(block_id);
    ASSERT_TRUE(it != bm->block_map_.end());
    NSBlock* block = it->second;
    ASSERT_TRUE(block != NULL);
    int32_t cs1 = 23;
    int32_t cs2 = 45;
    int32_t cs3 = 67;
    int32_t cs4 = 89;
    bool ret =
        bm->UpdateBlockInfo(block_id, cs1, block_size, block_version) &&
        bm->UpdateBlockInfo(block_id, cs2, block_size, block_version) &&
        bm->UpdateBlockInfo(block_id, cs3, block_size, block_version);
    ASSERT_TRUE(ret);
    ASSERT_EQ(block->version, block_version);
    ASSERT_EQ(block->block_size, block_size);
    int64_t new_block_size = 4;
    int64_t new_block_version = 5;
    ret = bm->UpdateBlockInfo(block_id, cs4, new_block_size, new_block_version);
    ASSERT_TRUE(ret);
    ASSERT_EQ(block->version, new_block_version);
    ASSERT_EQ(block->block_size, new_block_size);
    ret = bm->UpdateBlockInfo(block_id, cs1, block_size, block_version) ||
          bm->UpdateBlockInfo(block_id, cs2, block_size, block_version);
    ASSERT_FALSE(ret);
    ret = bm->UpdateBlockInfo(block_id, cs3, block_size, block_version) &&
          bm->UpdateBlockInfo(block_id, cs4, block_size, block_version);
    ASSERT_TRUE(ret);
}

TEST_F(BlockMappingTest, NotRecoverEmptyBlock) {
    int64_t block_id = 1;
    int64_t block_version = 0;
    int64_t block_size = 0;
    int32_t replica = 3;
    BlockMapping* bm = new BlockMapping(&thread_pool);
    bm->RebuildBlock(block_id, replica, block_version, block_size);
    BlockMapping::NSBlockMap::iterator it = bm->block_map_.find(block_id);
    ASSERT_TRUE(it != bm->block_map_.end());
    NSBlock* block = it->second;
    ASSERT_TRUE(block != NULL);
    int32_t cs1 = 23;
    int32_t cs2 = 45;
    int32_t cs3 = 67;
    bool ret =
        bm->UpdateBlockInfo(block_id, cs1, block_size, block_version) &&
        bm->UpdateBlockInfo(block_id, cs2, block_size, block_version) &&
        bm->UpdateBlockInfo(block_id, cs3, block_size, block_version);
    ASSERT_TRUE(ret);
    ASSERT_EQ(block->version, block_version);
    ASSERT_EQ(block->block_size, block_size);
    bm->DealWithDeadBlock(cs1, block_id);
    ASSERT_TRUE(block->recover_stat == kLoRecover);
    bm->DealWithDeadBlock(cs2, block_id);
    ASSERT_TRUE(block->recover_stat == kHiRecover);
    bm->DealWithDeadBlock(cs3, block_id);
    ASSERT_TRUE(block->recover_stat == kLost);
    ASSERT_TRUE(bm->lo_pri_recover_.empty());
    ASSERT_TRUE(bm->hi_pri_recover_.empty());
    ASSERT_TRUE(bm->lost_blocks_.empty());
}

} // namespace bfs
} // namespace baidu

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
