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


} // namespace bfs
} // namespace baidu

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
