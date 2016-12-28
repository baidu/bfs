#define private public
#include "nameserver/file_lock_manager.h"

#include <functional>

#include <gtest/gtest.h>
#include <common/thread_pool.h>

baidu::bfs::FileLockManager flm;

namespace baidu {
namespace bfs {

class FileLockManagerTest : public ::testing::Test {
public:
    FileLockManagerTest() {}
};

void WriteLock(const std::string& file_path, bool unlock = false) {
    flm.WriteLock(file_path);
    sleep(1);
    if (unlock) {
        flm.Unlock(file_path);
    }
}

void ReadLock(const std::string& file_path, bool unlock = false) {
    flm.ReadLock(file_path);
    sleep(1);
    if (unlock) {
        flm.Unlock(file_path);
    }
}

void Unlock(const std::string& file_path) {
    flm.Unlock(file_path);
}

TEST_F(FileLockManagerTest, Basic) {
    std::string file_path1 = "/home/dir1/file1";
    std::string file_path2 = "/home/dir2/file2";

    flm.WriteLock(file_path1);
    flm.Unlock(file_path1);

    flm.ReadLock(file_path1);
    flm.Unlock(file_path1);

    flm.ReadLock(file_path1);
    flm.ReadLock(file_path1);
    flm.Unlock(file_path1);
    flm.Unlock(file_path1);

    flm.WriteLock("/");
    flm.Unlock("/");

    flm.ReadLock("/");
    flm.Unlock("/");
}

TEST_F(FileLockManagerTest, RandomReadWriteLock) {
    baidu::common::ThreadPool thread_pool;
    srand(time(NULL));
    std::string file_path = "/home/dir1/file1";
    for (int i = 0; i < 10; i++) {
        int r = rand() % 2;
        if (r == 1) {
            thread_pool.AddTask(std::bind(WriteLock, file_path, true));
        } else {
            thread_pool.AddTask(std::bind(ReadLock, file_path, true));
        }
    }
    thread_pool.Stop(true);
    for (size_t i = 0; i < flm.locks_.size(); i++) {
        FileLockManager::LockBucket* l = flm.locks_[i];
        ASSERT_EQ(l->lock_map.size(), 0);
    }
}

TEST_F(FileLockManagerTest, UnlockInAnotherThread) {
    baidu::common::ThreadPool thread_pool;
    std::string file_path = "/home/dir1/file1";
    thread_pool.AddTask(std::bind(WriteLock, file_path, false));
    // wait for task to be executed
    thread_pool.Stop(true);
    Unlock(file_path);
}

TEST_F(FileLockManagerTest, NormailzeLockPath) {
    baidu::common::ThreadPool thread_pool(1);
    std::string lock_file_path = "/home/dir1/file1/";
    std::string unlock_file_path = "//home//dir1//file1//";
    thread_pool.AddTask(std::bind(WriteLock, lock_file_path, false));
    // wait for task to be executed
    thread_pool.Stop(true);
    Unlock(unlock_file_path);
    for (size_t i = 0; i < flm.locks_.size(); i++) {
        FileLockManager::LockBucket* l = flm.locks_[i];
        ASSERT_EQ(l->lock_map.size(), 0);
    }
}

} // namespace bfs
} // namespace baidu

int main(int argc, char** argv) {
    ::baidu::common::SetLogLevel(2);
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
