// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//

#include <iostream>
#include <functional>
#include <stdio.h>

#include <common/timer.h>
#include <common/thread.h>
#include "nameserver/logdb.h"

namespace baidu {
namespace bfs {

void DumpMarker(char* path) {
    FILE* fp = fopen(path, "r");
    MarkerEntry mark;
    std::string data;
    int64_t v;
    while (true) {
        int ret = LogDB::ReadOne(fp, &data);
        if (ret == 0)  break;
        if (ret < 0) {
            std::cerr << "DumpMarker failed while reading" << std::endl;
            return;
        }
        LogDB::DecodeMarker(data, &mark);
        memcpy(&v, &(mark.value[0]), 8);
        std::cout << mark.key << "\t->\t" << v << " ---- " << mark.value << std::endl;
    }
    fclose(fp);
}

void DumpLog(char* path) {
    FILE* fp = fopen(path, "r");
    std::string data;
    int64_t i = 0;
    while (true) {
        int ret = LogDB::ReadOne(fp, &data);
        if(ret == 0) break;
        if(ret < 0 ) {
            std::cerr << "DumpLog failed while reading" << std::endl;
            return;
        }
        std::cout << i << "\t->\t" << data << std::endl;
        ++i;
    }
    fclose(fp);
}

void DumpIdx(char* path) {
    FILE* fp = fopen(path, "r");
    char buf[16];
    int64_t index, offset;
    while (true) {
        int ret = fread(buf, 1, 16, fp);
        if (ret == 0) break;
        if (ret < 0) {
            std::cerr << "DumpIdx failed while reading" << std::endl;
            return;
        }
        memcpy(&index, buf, 8);
        memcpy(&offset, buf + 8, 8);
        std::cout << index << "\t->\t" << offset << std::endl;
    }
    fclose(fp);
}

void WriteHelper(int start, int end, const std::string& str, LogDB* logdb) {
    for (int i = start; i < end; ++i) {
        logdb->Write(i, str);
    }
    std::cerr << common::timer::get_micros() << std::endl;
}

void ReadHelper(int start, int end, const std::string& str, LogDB* logdb) {
    std::string res;
    for (int i = start; i < end; ++i) {
        logdb->Read(i, &res);
        assert(res == str);
    }
    std::cerr << common::timer::get_micros() << std::endl;
}

void Test(int n, int l) {
    LogDB* logdb;
    LogDB::Open("./dbtest", DBOption(), &logdb);

    int64_t start = common::timer::get_micros();
    std::string str(l, 'a');
    WriteHelper(0, n, str, logdb);
    int64_t now = common::timer::get_micros();
    double rate = double(n) / double(now - start);
    std::cerr << rate * 1000000.0 << std::endl;

    start = common::timer::get_micros();
    common::Thread w;
    w.Start(std::bind(&WriteHelper, n, n + n, str, logdb));
    common::Thread r;
    r.Start(std::bind(&ReadHelper, 0, n, str, logdb));
    w.Join();
    r.Join();
    now = common::timer::get_micros();
    rate = double(n) / double(now - start);
    std::cerr << rate * 1000000.0 << std::endl;

    //system("rm -rf ./dbtest");
}

} // namespace bfs
} // namespace baidu

int main(int argc, char* argv[]) {
    std::string path(argv[1]);
    if (path.find(".mak") != std::string::npos || "marker.tmp" == path) {
        baidu::bfs::DumpMarker(argv[1]);
    } else if (path.find(".log") != std::string::npos) {
        baidu::bfs::DumpLog(argv[1]);
    } else if (path.find(".idx") != std::string::npos) {
        baidu::bfs::DumpIdx(argv[1]);
    } else if (path == "test") {
        std::string n(argv[2]);
        std::string l(argv[3]);
        baidu::bfs::Test(std::stoi(n), std::stoi(l));
    }
}
