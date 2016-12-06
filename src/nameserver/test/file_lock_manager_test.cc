#include "nameserver/file_lock_manager.h"

#include <functional>

#include <gtest/gtest.h>
#include <common/thread_pool.h>

baidu::bfs::FileLockManager flm;
baidu::common::ThreadPool thread_pool;

namespace baidu {
namespace bfs {

class FileLockManagerTest : public ::testing::Test {
public:
    FileLockManagerTest() {}
};

void WriteLock(const std::string& file_path) {
    flm.WriteLock(file_path);
    sleep(1);
    flm.Unlock(file_path);
}

void ReadLock(const std::string& file_path) {
    flm.ReadLock(file_path);
    sleep(1);
    flm.Unlock(file_path);
}

TEST_F(FileLockManagerTest, Basic) {
    thread_pool.AddTask(std::bind(WriteLock, "/home/dir1/file1"));
    thread_pool.AddTask(std::bind(ReadLock, "/home/dir1/file1"));
    thread_pool.AddTask(std::bind(WriteLock, "/home/dir1/file1"));
    thread_pool.AddTask(std::bind(ReadLock, "/home/dir1/file1"));
    thread_pool.AddTask(std::bind(WriteLock, "/home/dir1/"));
    thread_pool.AddTask(std::bind(WriteLock, "/home/dir1/file2"));
    thread_pool.AddTask(std::bind(ReadLock, "/home/dir1/file2"));
}

} // namespace bfs
} // namespace baidu

int main(int argc, char** argv) {
    ::baidu::common::SetLogLevel(2);
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
