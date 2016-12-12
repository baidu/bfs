// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//

#define private public

#include <nameserver/file_lock.h>

#include <gtest/gtest.h>

namespace baidu {
namespace bfs {

class FileLockTest : public ::testing::Test {
public:
    FileLockTest() {}
protected:
}; 
FileLockManager file_lock_manager;

void SetFileLockManager() {
    WriteLock::file_lock_manager_ = &file_lock_manager;
    ReadLock::file_lock_manager_ = &file_lock_manager;
}

TEST_F(FileLockTest, WriteLockForOneFile) {
    WriteLockGuard guard1(new WriteLock("/home/dir1/file1"));
    WriteLockGuard guard2(new WriteLock("/home/dir1/file2"));
}

TEST_F(FileLockTest, WriteLockForTwoFile) {
    WriteLockGuard guard2(new WriteLock("/home/dir1/file1", "/home/dir1/file2"));
}

TEST_F(FileLockTest, ReadLock) {
    ReadLockGuard guard1(new ReadLock("/home/dir1/file1"));
    ReadLockGuard guard2(new ReadLock("/home/dir1/file2"));
}

} // namespace bfs
} // namespace baidu

int main(int argc, char** argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    baidu::bfs::SetFileLockManager();
    baidu::common::SetLogLevel(2);
    return RUN_ALL_TESTS();
}
