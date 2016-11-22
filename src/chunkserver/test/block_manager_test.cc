// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//

#define private public
#include "chunkserver/block_manager.h"
#include "chunkserver/data_block.h"

#include <gflags/gflags.h>
#include <gtest/gtest.h>

DECLARE_string(namedb_path);
DECLARE_int32(write_buf_size);


namespace baidu {
namespace bfs {

void sleep_task() {
    sleep(1);
}

void write_task(Block* block, int32_t seq, int64_t offset, std::string write_data) {
    block->Write(seq, offset, write_data.data(), write_data.size(), NULL);
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
    int64_t sync_time;
    StatusCode status;
    Block* block = block_manager.CreateBlock(block_id, &sync_time, &status);
    ASSERT_TRUE(block != NULL);
    std::string disk_file_path = block->disk_file_;
    // now ref count for block is 2
    ret = block->Write(0, 0, NULL, 0, NULL);
    ASSERT_TRUE(ret);
    FLAGS_write_buf_size = 5;
    std::string test_write_data("hello world");
    ret = block->Write(1, 0, test_write_data.data(), test_write_data.size(), NULL);
    ASSERT_TRUE(ret);
    block_manager.CloseBlock(block);
    //after RemoveBlock, ref count for block is 1
    block_manager.RemoveBlock(block_id);
    ASSERT_EQ(block->refs_, 1);
    ASSERT_EQ(block->deleted_, true);
    struct stat st;
    ASSERT_TRUE(stat(disk_file_path.c_str(), &st) == 0);
    //after the last ref is released, disk file should be removed
    block->DecRef();
    ASSERT_TRUE(stat(disk_file_path.c_str(), &st) != 0);
    ASSERT_EQ(errno, ENOENT);

    // close before write
    block_id = 456;
    block = block_manager.CreateBlock(block_id, &sync_time, &status);
    ASSERT_TRUE(block != NULL);
    block_manager.CloseBlock(block);
    ASSERT_EQ(block->finished_, true);
    ASSERT_EQ(block->file_desc_, Block::kClosed);
    block_manager.RemoveBlock(block_id);
    ASSERT_EQ(block->refs_, 1);
    ASSERT_EQ(block->deleted_, 1);
    disk_file_path = block->disk_file_;
    block->DecRef();
    ASSERT_TRUE(stat(disk_file_path.c_str(), &st) != 0);
    ASSERT_EQ(errno, ENOENT);

    // delete before write
    block_id = 789;
    block = block_manager.CreateBlock(block_id, &sync_time, &status);
    ASSERT_TRUE(block != NULL);
    block_manager.RemoveBlock(block_id);
    ASSERT_EQ(block->refs_, 1);
    ASSERT_EQ(block->deleted_, true);
    ASSERT_EQ(block->finished_, false);
    ASSERT_EQ(block->file_desc_, Block::kNotCreated);
    // Write will fail
    ret = block->Write(0, 0, NULL, 0, NULL);
    ASSERT_EQ(ret, false);
    disk_file_path = block->meta_.store_path() + Block::BuildFilePath(block_id);
    ASSERT_TRUE(stat(disk_file_path.c_str(), &st) != 0);
    ASSERT_EQ(errno, ENOENT);
    block->DecRef();
    ASSERT_TRUE(stat(disk_file_path.c_str(), &st) != 0);
    ASSERT_EQ(errno, ENOENT);

    rmdir("./test_dir");
}

TEST_F(BlockManagerTest, Out_of_order) {
    ThreadPool thread_pool(10);
    mkdir("./test_dir", S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
    BlockManager block_manager("./test_dir");
    bool ret = block_manager.LoadStorage();
    ASSERT_TRUE(ret);
    StatusCode status;
    int64_t block_id = 123;
    int64_t sync_time;
    //after create, ref for this block is 2
    Block* block = block_manager.CreateBlock(block_id, &sync_time, &status);
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

    std::function<void()> close_task = std::bind(&BlockManager::CloseBlock, &block_manager, block);
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
    rmdir("./test_dir");
}

}
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
