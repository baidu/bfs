// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//

#include <leveldb/db.h>
#include <common/util.h>
#include <iostream>
#include <string>

#include "proto/file.pb.h"

namespace baidu {
namespace bfs {

void ScanNamespace(const std::string db_path) {
    leveldb::DB* db;
    leveldb::Status s = leveldb::DB::Open(leveldb::Options(), db_path, &db);
    if (!s.ok()) {
        db = NULL;
        std::cerr << "Open leveldb fail " << db_path << std::endl;
        return;
    }

    leveldb::Iterator* it = db->NewIterator(leveldb::ReadOptions());
    for (it->Seek(std::string(7, '\0') + '\1'); it->Valid(); it->Next()) {
        std::string key = it->key().ToString();
        uint64_t entry_id = common::util::DecodeBigEndian(key.c_str());
        FileInfo info;
        bool ret = info.ParseFromArray(it->value().data(), it->value().size());
        if (!ret) {
            std::cerr << "Parse failed " << entry_id << "-" << key.substr(8) << std::endl;
            return;
        }
        std::cout << entry_id << "-" << key.substr(8) << " "
                  << info.entry_id() << " " << info.name() << std::endl;
    }
}

} // namespace bfs
} //namespace baidu

int main(int argc, char* argv[]) {
    if (argc != 2) {
        std::cerr << "Usage:\n./ns_dump db_path" << std::endl;
        return -1;
    }
    std::string path(argv[1]);
    baidu::bfs::ScanNamespace(path);
}
