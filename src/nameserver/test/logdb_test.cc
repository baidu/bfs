#define private public
#include "nameserver/logdb.h"

#include <iostream>
#include <vector>
#include <sys/stat.h>
#include <gtest/gtest.h>
#include <boost/bind.hpp>
#include <boost/function.hpp>

#include <common/string_util.h>
#include <common/thread.h>

namespace baidu {
namespace bfs {

class LogDBTest : public ::testing::Test {
public:
    LogDBTest() {}
protected:
};

DBOption option;

void WriteMarker_Helper(const std::string& key, int n, LogDB* logdb) {
    for (int i = 0; i < n; ++i) {
        logdb->WriteMarker(key, i);
    }
}

TEST_F(LogDBTest, EncodeLogEntry) {
    LogDB logdb(option);
    LogDataEntry entry(3, "helloworld");
    std::string str;
    logdb.EncodeLogEntry(entry, &str);
    ASSERT_EQ(str.length(), 22U);
    LogDataEntry decode_entry;
    logdb.DecodeLogEntry(str.substr(4), &decode_entry);
    ASSERT_EQ(decode_entry.index, 3);
    ASSERT_EQ(decode_entry.entry, "helloworld");
}

TEST_F(LogDBTest, EncodeMarker) {
    LogDB logdb(option);
    MarkerEntry marker("key", "value");
    std::string str;
    logdb.EncodeMarker(marker, &str);
    ASSERT_EQ(str.length(), 20U);
    MarkerEntry decode_marker;
    logdb.DecodeMarker(str.substr(4), &decode_marker);
    ASSERT_EQ(decode_marker.key, "key");
    ASSERT_EQ(decode_marker.value, "value");
}

TEST_F(LogDBTest, ReadOne) {
    LogDB logdb(option);
    LogDataEntry entry(3, "helloworld");
    std::string str;
    logdb.EncodeLogEntry(entry, &str);

    FILE* fp = fopen("marker.mak", "w");
    fwrite(str.c_str(), 1, str.length(), fp);
    fclose(fp);
    fp = fopen("marker.mak", "r");
    std::string res;
    logdb.ReadOne(fp, &res);
    fclose(fp);
    ASSERT_EQ(res, str.substr(4));

    fp = fopen("marker.mak", "a");
    fwrite("foo", 1, 3, fp);
    fclose(fp);
    fp = fopen("marker.mak", "r");
    ASSERT_EQ(logdb.ReadOne(fp, &res), 18);
    ASSERT_EQ(logdb.ReadOne(fp, &res), -1);

    remove("marker.mak");
}

TEST_F(LogDBTest, WriteMarker) {
    // write then read
    LogDB* logdb = new LogDB(option);
    WriteMarker_Helper("mark1", 10, logdb);
    int64_t v;
    logdb->ReadMarker("mark1", &v);
    ASSERT_EQ(v, 9);
    delete logdb;

    // test recover
    logdb = new LogDB(option);
    logdb->ReadMarker("mark1", &v);
    ASSERT_EQ(v, 9);

    // concurrency test
    std::vector<common::Thread*> threads;
    for (int i = 0; i < 10; ++i) {
        common::Thread* t = new common::Thread();
        t->Start(boost::bind(&WriteMarker_Helper, common::NumToString(i), 100, logdb));
        threads.push_back(t);
    }
    for (int i = 0; i < 10; ++i) {
        threads[i]->Join();
        delete threads[i];
    }
    for (int i = 0; i < 10; ++i) {
        int64_t v;
        logdb->ReadMarker(common::NumToString(i), &v);
        ASSERT_EQ(99, v);
    }
    delete logdb;

    logdb = new LogDB(option);
    for (int i = 0; i < 10; ++i) {
        int64_t v;
        logdb->ReadMarker(common::NumToString(i), &v);
        ASSERT_EQ(99, v);
    }
    delete logdb;
    remove("marker.mak");
}

TEST_F(LogDBTest, WriteMarkerSnapshot) {
    DBOption option;
    option.snapshot_interval = 300;
    LogDB logdb(option);
    WriteMarker_Helper("mark", 1000000, &logdb);
    usleep(300000);
    int64_t v;
    logdb.ReadMarker("mark", &v);
    ASSERT_EQ(999999, v);
    struct stat sta;
    ASSERT_EQ(0, lstat("marker.mak", &sta));
    ASSERT_EQ(24, sta.st_size);
    remove("marker.mak");
}

} // namespace bfs
} // namespace baidu

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
