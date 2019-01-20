// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//

#define private public
#include "nameserver/block_mapping_manager.h"

#include <gtest/gtest.h>

namespace baidu {
namespace bfs {

class BlockMappingManagerTest : public ::testing::Test {
public:
    BlockMappingManagerTest() {}
protected:
};

TEST_F(BlockMappingManagerTest, CheckBlocksClosed) {
    BlockMappingManager bm(5);
    int64_t block_id1 = 1;
    int64_t block_id2 = 2;
    int64_t block_version = 0;
    int64_t block_size = 0;
    int32_t replica = 3;
    bm.RebuildBlock(block_id1, replica, block_version, block_size);
    bm.RebuildBlock(block_id2, replica, block_version, block_size);
    std::vector<int64_t> blocks;
    blocks.push_back(block_id1);
    blocks.push_back(block_id2);
    bool ret = bm.CheckBlocksClosed(blocks);
    int32_t cs1 = 23;
    int32_t cs2 = 45;
    int32_t cs3 = 67;
    ret =
        bm.UpdateBlockInfo(block_id1, cs1, block_size, block_version) &&
        bm.UpdateBlockInfo(block_id1, cs2, block_size, block_version) &&
        bm.UpdateBlockInfo(block_id1, cs3, block_size, block_version);
    ASSERT_EQ(ret, true);
    ret = bm.CheckBlocksClosed(blocks);
    ASSERT_EQ(ret, false);
    ret =
        bm.UpdateBlockInfo(block_id2, cs1, block_size, block_version) &&
        bm.UpdateBlockInfo(block_id2, cs2, block_size, block_version) &&
        bm.UpdateBlockInfo(block_id2, cs3, block_size, block_version);
    ASSERT_EQ(ret, true);
    ret = bm.CheckBlocksClosed(blocks);
    ASSERT_EQ(ret, true);
}

} // namespace bfs
} // namespace baidu

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

