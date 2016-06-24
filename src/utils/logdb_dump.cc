// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//

#include <iostream>
#include <stdio.h>
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
    LogDataEntry entry;
    std::string data;
    while (true) {
        int ret = LogDB::ReadOne(fp, &data);
        if(ret == 0) break;
        if(ret < 0 ) {
            std::cerr << "DumpLog failed while reading" << std::endl;
            return;
        }
        LogDB::DecodeLogEntry(data, &entry);
        std::cout << entry.index << "\t->\t" << entry.entry << std::endl;
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
    }
}
