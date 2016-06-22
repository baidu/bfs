#define private public

#include <iostream>
#include <vector>
#include <sys/stat.h>
#include <gtest/gtest.h>
#include <boost/bind.hpp>
#include <boost/function.hpp>

#include <common/string_util.h>
#include <common/thread.h>

#include "nameserver/logdb.h"
#include "proto/status_code.pb.h"

namespace baidu {
namespace bfs {

class LogDBTest : public ::testing::Test {
public:
    LogDBTest() {
        option.path = "./dbtest";
        system("rm -rf ./dbtest");
    }
protected:
    DBOption option;
};

void WriteMarker_Helper(const std::string& key, int n, LogDB* logdb) {
    for (int i = 0; i < n; ++i) {
        logdb->WriteMarker(key, i);
    }
}

void WriteLog_Helper(int start, int n, LogDB* logdb) {
    for (int i = start; i < start + n; ++i) {
        logdb->Write(i, common::NumToString(i) + "test");
    }
}

void ReadLog_Helper(int start, int n , LogDB* logdb) {
    std::string log;
    for (int i = start; i < start + n; ++i) {
        logdb->Read(i, &log);
        ASSERT_EQ(log, common::NumToString(i) + "test");
    }
}

TEST_F(LogDBTest, EncodeLogEntry) {
    LogDB logdb(option);
    LogDataEntry entry(3, "helloworld");
    std::string str;
    logdb.EncodeLogEntry(entry, &str);
    ASSERT_EQ(str.length(), 18U);
    LogDataEntry decode_entry;
    logdb.DecodeLogEntry(str, &decode_entry);
    ASSERT_EQ(decode_entry.index, 3);
    ASSERT_EQ(decode_entry.entry, "helloworld");
}

TEST_F(LogDBTest, EncodeMarker) {
    LogDB logdb(option);
    MarkerEntry marker("key", "value");
    std::string str;
    logdb.EncodeMarker(marker, &str);
    ASSERT_EQ(str.length(), 16U);
    MarkerEntry decode_marker;
    logdb.DecodeMarker(str, &decode_marker);
    ASSERT_EQ(decode_marker.key, "key");
    ASSERT_EQ(decode_marker.value, "value");
}


TEST_F(LogDBTest, ReadOne) {
    LogDB logdb(option);
    LogDataEntry entry(0, "helloworld");
    std::string str;
    int32_t len = 18;
    str.append(reinterpret_cast<char*>(&len), 4);
    logdb.EncodeLogEntry(entry, &str);

    FILE* fp = fopen((option.path + "/0.log").c_str(), "w");
    fwrite(str.c_str(), 1, str.length(), fp);
    fclose(fp);
    fp = fopen((option.path + "/0.log").c_str(), "r");
    std::string res;
    logdb.ReadOne(fp, &res);
    fclose(fp);
    ASSERT_EQ(res, str.substr(4));

    fp = fopen((option.path + "/0.log").c_str(), "a");
    fwrite("foo", 1, 3, fp);
    fclose(fp);
    fp = fopen((option.path + "/0.log").c_str(), "r");
    ASSERT_EQ(logdb.ReadOne(fp, &res), 18);
    ASSERT_EQ(logdb.ReadOne(fp, &res), -1);
    fclose(fp);

    //system("rm -rf ./dbtest");
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
    system("rm -rf ./dbtest");
}

TEST_F(LogDBTest, WriteMarkerSnapshot) {
    DBOption option;
    option.snapshot_interval = 300;
    LogDB logdb(option);
    WriteMarker_Helper("mark", 500000, &logdb);
    usleep(300000);
    int64_t v;
    logdb.ReadMarker("mark", &v);
    ASSERT_EQ(499999, v);
    struct stat sta;
    ASSERT_EQ(0, lstat("marker.mak", &sta));
    ASSERT_EQ(24, sta.st_size);
    system("rm -rf ./dbtest");
}

TEST_F(LogDBTest, Write) {
    LogDB* logdb = new LogDB(option);
    WriteLog_Helper(0, 3, logdb);
    ReadLog_Helper(0, 3, logdb);

    // test build file cache
    delete logdb;
    logdb = new LogDB(option);
    WriteLog_Helper(3, 2, logdb);
    ReadLog_Helper(0, 5, logdb);
    std::string entry;
    ASSERT_EQ(logdb->Read(6, &entry), kNotFound);
    ASSERT_EQ(logdb->Write(1, "bad"), kBadParameter);
    ASSERT_EQ(logdb->Write(7, "bad"), kBadParameter);
    delete logdb;
    system("rm -rf ./dbtest");

    logdb = new LogDB(option);
    WriteLog_Helper(10, 5, logdb);
    ReadLog_Helper(10, 5, logdb);
    delete logdb;
    logdb = new LogDB(option);
    WriteLog_Helper(15, 5, logdb);
    ReadLog_Helper(15, 5, logdb);
    system("rm -rf ./dbtest");
}

TEST_F(LogDBTest, Read) {
    LogDB* logdb = new LogDB(option);
    WriteLog_Helper(0, 500, logdb);
    std::vector<common::Thread*> threads;
    for (int i = 0; i < 5; ++i) {
        common::Thread* t = new common::Thread();
        t->Start(boost::bind(&ReadLog_Helper, i * 100, 100, logdb));
        threads.push_back(t);
    }
    for (int i = 0; i < 5; ++i) {
        threads[i]->Join();
        delete threads[i];
    }
    delete logdb;
    system("rm -rf ./dbtest");
}

TEST_F(LogDBTest, NewWriteLog) {
    DBOption option;
    option.log_size = 1;
    option.path = "./dbtest";
    LogDB* logdb = new LogDB(option);
    WriteLog_Helper(0, 200000, logdb);
    // 0.log, 50462.log, 100377.log, 148040.log, 195703.log
    int ret = access("./dbtest/0.log", R_OK);
    ASSERT_EQ(ret, 0);
    ret = access("./dbtest/50462.log", R_OK);
    ASSERT_EQ(ret, 0);
    ret = access("./dbtest/100377.log", R_OK);
    ASSERT_EQ(ret, 0);
    ret = access("./dbtest/148040.log", R_OK);
    ASSERT_EQ(ret, 0);
    ret = access("./dbtest/195703.log", R_OK);
    ASSERT_EQ(ret, 0);
    ReadLog_Helper(0, 200000, logdb);

    delete logdb;
    system("rm -rf ./dbtest");
}

TEST_F(LogDBTest, DeleteUpTo) {
    DBOption option;
    option.log_size = 1;
    option.path = "./dbtest";
    LogDB* logdb = new LogDB(option);
    WriteLog_Helper(0, 200000, logdb);
    // 0.log, 50462.log, 100377.log, 148040.log, 195703.log
    logdb->DeleteUpTo(99999);
    int ret = access("./dbtest/0.log", R_OK);
    ASSERT_EQ(ret, -1);
    ret = access("./dbtest/50462.log", R_OK);
    ASSERT_EQ(ret, 0);
    ret = access("./dbtest/100377.log", R_OK);
    ASSERT_EQ(ret, 0);
    ret = access("./dbtest/148040.log", R_OK);
    ASSERT_EQ(ret, 0);
    ret = access("./dbtest/195703.log", R_OK);
    ASSERT_EQ(ret, 0);
    ASSERT_EQ(logdb->DeleteUpTo(99999), kBadParameter);
    ASSERT_EQ(logdb->DeleteUpTo(200001), kBadParameter);
    ReadLog_Helper(100000, 100000, logdb);

    delete logdb;
    system("rm -rf ./dbtest");
}

TEST_F(LogDBTest, DeleteFrom) {
    DBOption option;
    option.log_size = 1;
    option.path = "./dbtest";
    LogDB* logdb = new LogDB(option);
    WriteLog_Helper(0, 200000, logdb);
    // 0.log, 50462.log, 100377.log, 148040.log, 195703.log
    logdb->DeleteFrom(100500);
    int ret = access("./dbtest/0.log", R_OK);
    ASSERT_EQ(ret, 0);
    ret = access("./dbtest/50462.log", R_OK);
    ASSERT_EQ(ret, 0);
    ret = access("./dbtest/100377.log", R_OK);
    ASSERT_EQ(ret, 0);
    ret = access("./dbtest/148040.log", R_OK);
    ASSERT_EQ(ret, -1);
    ret = access("./dbtest/195703.log", R_OK);
    ASSERT_EQ(ret, -1);
    logdb->DeleteFrom(100378);
    ret = access("./dbtest/100377.log", R_OK);
    ASSERT_EQ(ret, 0);
    struct stat sta;
    lstat("./dbtest/100377.log", &sta);
    ASSERT_EQ(sta.st_size, 22);
    lstat("./dbtest/100377.idx", &sta);
    ASSERT_EQ(sta.st_size, 16);
    logdb->DeleteFrom(100377);
    ret = access("./dbtest/100377.log", R_OK);
    ASSERT_EQ(ret, -1);
    ASSERT_EQ(logdb->DeleteFrom(100377), kBadParameter);
    ASSERT_EQ(logdb->DeleteFrom(-1), kBadParameter);
    WriteLog_Helper(100377, 1, logdb);
    ReadLog_Helper(0, 100378, logdb);

    delete logdb;
    system("rm -rf ./dbtest");
}

} // namespace bfs
} // namespace baidu

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
