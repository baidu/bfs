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

void WriteLock(const std::string& file_path, bool unlock = true) {
    flm.WriteLock(file_path);
    sleep(1);
    if (unlock) {
        flm.Unlock(file_path);
    }
}

void ReadLock(const std::string& file_path, bool unlock) {
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
}

TEST_F(FileLockManagerTest, UnlockInAnotherThread) {
    baidu::common::ThreadPool thread_pool;
    std::string file_path = "/home/dir1/file1";
    thread_pool.AddTask(std::bind(WriteLock, file_path, false));
    // wait for task to be executed
    sleep(2);
    thread_pool.AddTask(std::bind(Unlock, file_path));
}

} // namespace bfs
} // namespace baidu

int main(int argc, char** argv) {
    ::baidu::common::SetLogLevel(2);
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
