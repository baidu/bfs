// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//

#include <leveldb/db.h>
#include <iostream>
#include <string>

#include <common/util.h>

#include "proto/file.pb.h"
#include "proto/block.pb.h"

namespace baidu {
namespace bfs {

bool ScanNamespace(const std::string& db_path) {
    leveldb::DB* db;
    leveldb::Status s = leveldb::DB::Open(leveldb::Options(), db_path, &db);
    if (!s.ok()) {
        db = NULL;
        std::cerr << "Open leveldb fail " << db_path << std::endl;
        return false;
    }

    leveldb::Iterator* it = db->NewIterator(leveldb::ReadOptions());
    for (it->Seek(std::string(7, '\0') + '\1'); it->Valid(); it->Next()) {
        std::string key = it->key().ToString();
        uint64_t entry_id = common::util::DecodeBigEndian64(key.c_str());
        FileInfo info;
        bool ret = info.ParseFromArray(it->value().data(), it->value().size());
        if (!ret) {
            std::cerr << "Try parse namespace failed: " << entry_id << "-" << key.substr(8) << std::endl;
            return false;
        }
        std::cout << info.DebugString() << std::endl;
    }
    return true;
}

void ScanChunkserver(const std::string& db_path) {
    leveldb::DB* db;
    leveldb::Status s = leveldb::DB::Open(leveldb::Options(), db_path, &db);
    if (!s.ok()) {
        db = NULL;
        std::cerr << "Open leveldb fail " << db_path << std::endl;
        return;
    }
    std::string version_key(8, '\0');
    version_key.append("version");
    leveldb::Iterator* it = db->NewIterator(leveldb::ReadOptions());
    for (it->Seek(std::string(8, '\0') + "version" + '\0'); it->Valid(); it->Next()) {
        BlockMeta meta;
        if (!meta.ParseFromArray(it->value().data(), it->value().size())) {
            std::cerr << "Try parse chunkserver failed: " << it->key().ToString() << " -> " << it->value().ToString() << std::endl;
            return;
        }
        std::cout << meta.DebugString() << std::endl;
    }
}

} // namespace bfs
} //namespace baidu

int main(int argc, char* argv[]) {
    if (argc != 2) {
        std::cerr << "Usage:\n./bfs_dump db_path" << std::endl;
        return -1;
    }
    std::string path(argv[1]);
    bool ret = baidu::bfs::ScanNamespace(path);
    if (!ret) {
        baidu::bfs::ScanChunkserver(path);
    }
}
