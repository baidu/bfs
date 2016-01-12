// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#define private public
#include "chunkserver/chunkserver_impl.h"

#include <gflags/gflags.h>
#include <gtest/gtest.h>

DECLARE_string(namedb_path);
DECLARE_string(block_store_path);

namespace baidu {
namespace bfs {

class ChunkserverImplTest : public ::testing::Test {
public:
    ChunkserverImplTest() {}
protected:
};

TEST_F(ChunkserverImplTest, ReadFile) {
    FLAGS_block_store_path = "./";
    ChunkServerImpl* cs = new ChunkServerImpl();
    delete cs;
}

}
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}





















/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
