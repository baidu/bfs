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

void WriteLock() {
    flm.WriteLock("/home/dir1/file1");
    sleep(1);
    flm.Unlock("/home/dir1/file1/");
}

void ReadLock() {
    flm.ReadLock("/home/dir1/file1");
    sleep(1);
    flm.Unlock("/home/dir1/file1/");
}

TEST_F(FileLockManagerTest, Basic) {
    thread_pool.AddTask(WriteLock);
    thread_pool.AddTask(ReadLock);
    thread_pool.AddTask(WriteLock);
    thread_pool.AddTask(ReadLock);
}

} // namespace bfs
} // namespace baidu

int main(int argc, char** argv) {
    ::baidu::common::SetLogLevel(2);
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
