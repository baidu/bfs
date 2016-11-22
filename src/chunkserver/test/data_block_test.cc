// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//

#include "chunkserver/data_block.h"
#include "chunkserver/file_cache.h"
#include "proto/block.pb.h"

#include <gflags/gflags.h>
#include <gtest/gtest.h>

namespace baidu {
namespace bfs {

class DataBlockTest : public ::testing::Test {
public:
    DataBlockTest() {}
protected:
};

TEST_F(DataBlockTest, CreateBlock) {
    mkdir("./block123", 0755);
    BlockMeta meta;
    ThreadPool thread_pool;
    FileCache file_cache(10);
    int64_t block_id = 123;
    meta.set_block_id(block_id);
    std::string file_path("./block123");
    meta.set_store_path(file_path);
    Block* block = new Block(meta, &thread_pool, &file_cache);
    ASSERT_TRUE(block != NULL);
    ASSERT_EQ(block->Id(), 123);
    ASSERT_EQ(block->Size(), 0);
    ASSERT_EQ(block->GetVersion(), -1);
    ASSERT_EQ(block->GetLastSeq(), -1);
    ASSERT_EQ(block->GetFilePath(), "./block123/123/0000000000");
    delete block;
    system("rm -rf ./block123");
}

TEST_F(DataBlockTest, WriteAndReadBlock) {
    BlockMeta meta;
    ThreadPool thread_pool;
    FileCache file_cache(10);
    int64_t block_id = 123;
    meta.set_block_id(block_id);
    mkdir("./block123", 0755);
    std::string file_path("./block123");
    meta.set_store_path(file_path);
    Block* block = new Block(meta, &thread_pool, &file_cache);
    block->AddRef();
    bool ret = block->Write(0, 0, NULL, 0);
    ASSERT_EQ(ret, true);
    std::string write_data("hello world");
    block->Write(1, 0, write_data.data(), write_data.size());
    ASSERT_EQ(ret, true);
    block->Write(0, 0, write_data.data(), write_data.size());
    ASSERT_EQ(ret, true);
    block->Write(-1, 0, write_data.data(), write_data.size());
    ASSERT_EQ(ret, true);
    block->SetSliceNum(2);
    ASSERT_TRUE(block->IsComplete());
    block->Close();
    ASSERT_TRUE(block->IsFinished());
    //check version & size
    ASSERT_EQ(block->GetLastSeq(), 1);
    ASSERT_EQ(block->Size(), write_data.size());
    // check disk file size
    struct stat st;
    int r = stat(block->GetFilePath().c_str(), &st);
    ASSERT_EQ(r, 0);
    ASSERT_EQ(st.st_size, write_data.size());
    //get meta for read
    meta = block->GetMeta();
    block->DecRef();
    //reopen this block for read
    block = new Block(meta, &thread_pool, &file_cache);
    block->AddRef();
    ASSERT_TRUE(block != NULL);
    char buf[128];
    memset(buf, 0, sizeof(buf));
    block->Read(buf, 100, 0);
    ASSERT_EQ(strcmp(buf, write_data.c_str()), 0);
    memset(buf, 0, sizeof(buf));
    block->Read(buf, 100, 5);
    ASSERT_EQ(buf, std::string(write_data.begin() + 5, write_data.end()));
    int len = block->Read(buf, 100, -1);
    ASSERT_EQ(len, -2);
    len = block->Read(buf, -1, 0);
    ASSERT_EQ(len, -1);
    block->DecRef();
    system("rm -rf ./block123");
}


}
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
