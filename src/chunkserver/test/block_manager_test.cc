// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//

#define private public
#include <iostream>
#include "chunkserver/block_manager.h"
#include "chunkserver/data_block.h"

#include <gflags/gflags.h>
#include <gtest/gtest.h>

DECLARE_string(namedb_path);
DECLARE_int32(write_buf_size);
DECLARE_bool(chunkserver_multi_path_on_one_disk);

namespace baidu {
namespace bfs {

void sleep_task() {
    sleep(1);
}

void write_task(Block* block, int32_t seq, int64_t offset, std::string write_data) {
    block->Write(seq, offset, write_data.data(), write_data.size(), NULL);
}

void create_block(int64_t start_id, int64_t end_id, BlockManager* bm) {
    for (int i = start_id; i <= end_id; ++i) {
        StatusCode status;
        Block* block = bm->CreateBlock(i, &status);
        ASSERT_TRUE(block != NULL);
        block->Write(0, 0, "some data", 9, NULL);
        bm->CloseBlock(block, true);
    }
}

class BlockManagerTest : public ::testing::Test {
public:
    BlockManagerTest() {}
protected:
};

TEST_F(BlockManagerTest, RemoveBlock) {
    mkdir("./test_dir", S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
    BlockManager block_manager("./test_dir");
    bool ret = block_manager.LoadStorage();
    ASSERT_TRUE(ret);

    //normal case
    int64_t block_id = 123;
    StatusCode status;
    Block* block = block_manager.CreateBlock(block_id, &status);
    ASSERT_TRUE(block != NULL);
    std::string disk_file_path = block->disk_file_;
    ret = block->Write(0, 0, NULL, 0, NULL);
    ASSERT_TRUE(ret);
    FLAGS_write_buf_size = 5;
    std::string test_write_data("hello world");
    ret = block->Write(1, 0, test_write_data.data(), test_write_data.size(), NULL);
    ASSERT_TRUE(ret);
    block_manager.CloseBlock(block, true);
    block_manager.RemoveBlock(block_id);
    ASSERT_EQ(block->deleted_, true);
    struct stat st;
    ASSERT_TRUE(stat(disk_file_path.c_str(), &st) == 0);
    block->DecRef();

    // close before write
    block_id = 456;
    block = block_manager.CreateBlock(block_id, &status);
    ASSERT_TRUE(block != NULL);
    block_manager.CloseBlock(block, true);
    ASSERT_EQ(block->finished_, true);
    ASSERT_EQ(block->file_desc_, Block::kClosed);
    block_manager.RemoveBlock(block_id);
    ASSERT_EQ(block->deleted_, 1);
    block->DecRef();

    // delete before write
    block_id = 789;
    block = block_manager.CreateBlock(block_id, &status);
    ASSERT_TRUE(block != NULL);
    block_manager.RemoveBlock(block_id);
    ASSERT_EQ(block->deleted_, true);
    ASSERT_EQ(block->finished_, false);
    ASSERT_EQ(block->file_desc_, Block::kNotCreated);
    // Write will fail
    ret = block->Write(0, 0, NULL, 0, NULL);
    ASSERT_EQ(ret, false);
    block->DecRef();

    system("rm -rf test_dir");
}

TEST_F(BlockManagerTest, Out_of_order) {
    ThreadPool thread_pool(10);
    mkdir("./test_dir", S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
    BlockManager block_manager("./test_dir");
    bool ret = block_manager.LoadStorage();
    ASSERT_TRUE(ret);
    StatusCode status;
    int64_t block_id = 123;
    //after create, ref for this block is 2
    Block* block = block_manager.CreateBlock(block_id, &status);
    ASSERT_TRUE(block != NULL);
    // we will use thread pool and closure to operate this block
    // first hold all threads
    for (int i = 0; i < 10; i++) {
        thread_pool.AddTask(std::bind(sleep_task));
    }
    //then add tasks to normal or priority queue, write/close/remove tasks will be out-of-order
    FLAGS_write_buf_size = 5;
    std::string test_data("hello world");
    srand(time(NULL));
    int32_t r = rand() % 2;
    std::function<void ()> write_data_task1 = std::bind(write_task, block, 0, 0, "");
    std::function<void ()> write_data_task2 = std::bind(write_task, block, 1, 0, test_data);
    if (r == 0) {
        thread_pool.AddTask(write_data_task1);
        thread_pool.AddTask(write_data_task2);
    } else {
        thread_pool.AddPriorityTask(write_data_task1);
        thread_pool.AddPriorityTask(write_data_task2);
    }

    std::function<void()> close_task = std::bind(&BlockManager::CloseBlock, &block_manager, block, true);
    r = rand() % 2;
    if (r == 0) {
        thread_pool.AddTask(close_task);
    } else {
        thread_pool.AddPriorityTask(close_task);
    }

    std::function<void()> remove_task = std::bind(&BlockManager::RemoveBlock, &block_manager, block_id);
    r = rand() % 2;
    if (r == 0) {
        thread_pool.AddTask(remove_task);
    } else {
        thread_pool.AddPriorityTask(remove_task);
    }

    //wait for all tasks run
    thread_pool.Stop(true);
    block->DecRef();
    system("rm -rf test_dir");
}

TEST_F(BlockManagerTest, ListBlocks) {
    FLAGS_chunkserver_multi_path_on_one_disk = true;
    std::string store_path = "./data1,./data2,./data3";
    mkdir("./data1", S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
    mkdir("./data2", S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
    mkdir("./data3", S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
    BlockManager block_manager(store_path);
    bool ret = block_manager.LoadStorage();
    ASSERT_TRUE(ret);
    create_block(1, 50, &block_manager);
    std::vector<BlockMeta> blocks;
    int64_t id = block_manager.ListBlocks(&blocks, 0, 100);
    assert(id == 50);
    assert(blocks.size() == 50);
    blocks.clear();

    id = block_manager.ListBlocks(&blocks, 26, 100);
    assert(id == 50);
    assert(blocks.size() == 25);
    blocks.clear();

    for (int i = 1; i <= 50; ++i) {
        if (i % 4 == 0 || i % 5 == 0) {
            block_manager.RemoveBlock(i);
        }
    }

    id = block_manager.ListBlocks(&blocks, 0, 100);
    assert(id == 49);
    assert(blocks.size() == 30);
    blocks.clear();
    system("rm -rf ./data1");
    system("rm -rf ./data2");
    system("rm -rf ./data3");
}

TEST_F(BlockManagerTest, RemoveDisk) {
    FLAGS_chunkserver_multi_path_on_one_disk = true;
    std::string store_path = "./data1,./data2,./data3";
    mkdir("./data1", S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
    mkdir("./data2", S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
    mkdir("./data3", S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
    BlockManager* block_manager = new BlockManager(store_path);
    bool ret = block_manager->LoadStorage();
    ASSERT_TRUE(ret);
    create_block(1, 50, block_manager);
    sleep(1);

    // reboost
    delete block_manager;
    block_manager = new BlockManager(store_path);
    ret = block_manager->LoadStorage();
    ASSERT_TRUE(ret);
    std::vector<BlockMeta> blocks;
    int64_t id = block_manager->ListBlocks(&blocks, 0, 100);
    assert(id == 50);
    assert(blocks.size() == 50);
    blocks.clear();

    // lose one disk
    delete block_manager;
    system("rm -rf ./data1");
    block_manager = new BlockManager(store_path);
    ret = block_manager->LoadStorage();
    ASSERT_TRUE(ret);
    id = block_manager->ListBlocks(&blocks, 0, 100);
    assert(blocks.size() < 50);
    blocks.clear();
    create_block(51, 60, block_manager);
    delete block_manager;
    system("rm -rf ./data2");
    system("rm -rf ./data3");
}

TEST_F(BlockManagerTest, WrongNsVersion) {
    FLAGS_chunkserver_multi_path_on_one_disk = true;
    std::string store_path = "./data1,./data2,./data3";
    mkdir("./data1", S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
    mkdir("./data2", S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
    mkdir("./data3", S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
    BlockManager* block_manager = new BlockManager(store_path);
    bool ret = block_manager->LoadStorage();
    ASSERT_TRUE(ret);
    block_manager->CleanUp(1);
    block_manager->SetNamespaceVersion(1);
    create_block(1, 20, block_manager);
    sleep(1);

    delete block_manager;
    std::string store_path2 = "./data1,./data2";
    block_manager = new BlockManager(store_path2);
    ret = block_manager->LoadStorage();
    ASSERT_TRUE(ret);
    block_manager->CleanUp(2);
    block_manager->SetNamespaceVersion(2);
    create_block(21, 40, block_manager);
    sleep(1);
    std::vector<BlockMeta> blocks;
    int64_t id = block_manager->ListBlocks(&blocks, 0, 100);
    assert(id == 40);
    assert(blocks.size() < 40);
    blocks.clear();

    delete block_manager;
    block_manager = new BlockManager(store_path);
    ret = block_manager->LoadStorage();
    ASSERT_TRUE(ret);
    ASSERT_TRUE(block_manager->NamespaceVersion() == -1);
    block_manager->CleanUp(2);
    block_manager->SetNamespaceVersion(2);
    id = block_manager->ListBlocks(&blocks, 0, 100);
    assert(id == 40);
    assert(blocks.size() == 20);
    blocks.clear();

    system("rm -rf ./data1");
    system("rm -rf ./data2");
    system("rm -rf ./data3");
}

}
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
