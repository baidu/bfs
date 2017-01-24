// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//

#include <string>
#include <map>

#include <gflags/gflags.h>
#include <leveldb/db.h>

#include <common/string_util.h>
#include <common/logging.h>

#include "proto/block.pb.h"

#include "utils/meta_converter.h"

namespace baidu {
namespace bfs {

const int CHUNKSERVER_META_VERSION = 1;

const int EMPTY_META = -1;

void CheckChunkserverMeta(const std::vector<std::string>& store_path_list) {
    std::map<std::string, leveldb::DB*> meta_dbs;
    int meta_version = EMPTY_META;
    for (size_t i = 0; i < store_path_list.size(); ++i) {
        const std::string& path = store_path_list[i];
        leveldb::Options options;
        options.create_if_missing = true;
        leveldb::DB* metadb;
        leveldb::Status s = leveldb::DB::Open(options, path + "/meta/", &metadb);
        if (!s.ok()) {
            LOG(ERROR, "[MetaCheck] Open meta on %s failed: %s", path.c_str(), s.ToString().c_str());
            exit(EXIT_FAILURE);
            return;
        }

        std::string version_key(8, '\0');
        version_key.append("version");
        std::string version_str;
        s = metadb->Get(leveldb::ReadOptions(), version_key, &version_str);
        if (s.IsNotFound()) {
            LOG(INFO, "[MetaCheck] Load empty meta %s", path.c_str());
        } else {
            int64_t ns_v = *(reinterpret_cast<int64_t*>(&version_str[0]));
            LOG(INFO, "[MetaCheck] Load namespace %ld on %s", ns_v, path.c_str());
            std::string meta_key(8, '\0');
            meta_key.append("meta");
            std::string meta_str;
            s = metadb->Get(leveldb::ReadOptions(), meta_key, &meta_str);
            if (s.ok()) {
                int cur_version = *(reinterpret_cast<int*>(&meta_str[0]));
                LOG(INFO, "[MetaCheck] %s Load meta version %d", path.c_str(), cur_version);
                if (meta_version != EMPTY_META && cur_version != meta_version) {
                    LOG(ERROR, "Cannot handle this situation!!!");
                    exit(EXIT_FAILURE);
                }
                meta_version = cur_version;
            } else if (s.IsNotFound()) {
                if (meta_version != EMPTY_META && meta_version != 0) {
                    LOG(ERROR, "Cannot handle this situation!!!");
                    exit(EXIT_FAILURE);
                }
                meta_version = 0;
                LOG(INFO, "No meta version %s", path.c_str());
            }
        }
        meta_dbs[path] = metadb;
    }
    if (meta_version == CHUNKSERVER_META_VERSION) {
        LOG(INFO, "[MetaCheck] Chunkserver meta check pass");
    } else if (meta_version == EMPTY_META) {
        LOG(INFO, "[MetaCheck] Chunkserver empty");
    } else if (meta_version == 0) {
        ChunkserverMetaV02V1(meta_dbs);
    } else {
        LOG(ERROR, "[MetaCheck] Cannot handle this situation!!!");
        exit(EXIT_FAILURE);
    }
    SetChunkserverMetaVersion(meta_dbs);
    CloseMetaStore(meta_dbs);
}

void ChunkserverMetaV02V1(const std::map<std::string, leveldb::DB*>& meta_dbs) {
    LOG(INFO, "[MetaCheck] Start converting chunkserver meta from verion 0");
    leveldb::DB* src_meta = NULL;
    std::string src_meta_path;
    std::string version_key(8, '\0');
    version_key.append("version");
    std::string version_str;
    for (auto it = meta_dbs.begin(); it != meta_dbs.end(); ++it) {
        leveldb::DB* cur_db = it->second;
        const std::string& cur_path = it->first;
        leveldb::Status s = cur_db->Get(leveldb::ReadOptions(), version_key, &version_str);
        if (s.ok()) {
            src_meta = cur_db;
            src_meta_path = it->first;
            LOG(INFO, "[MetaCheck] Source meta store %s", cur_path.c_str());
            break;
        } else {
            LOG(INFO, "[MetaCheck] No namespace version on %s, %s", cur_path.c_str(), s.ToString().c_str());
        }
    }
    if (!src_meta) {
        LOG(ERROR, "[MetaCheck] Cannot find a valid meta store");
        exit(EXIT_FAILURE);
    }

    leveldb::Iterator* it = src_meta->NewIterator(leveldb::ReadOptions());
    for (it->Seek(std::string(8, '\0') + "version" + '\0'); it->Valid(); it->Next()) {
        BlockMeta meta;
        if (!meta.ParseFromArray(it->value().data(), it->value().size())) {
            LOG(ERROR, "[MetaCheck] Parse BlockMeta failed: key = %s", it->key().ToString().c_str());
            exit(EXIT_FAILURE);
        }
        const std::string& path = meta.store_path();
        auto db_it = meta_dbs.find(path);
        if (db_it == meta_dbs.end()) {
            src_meta->Delete(leveldb::WriteOptions(), it->key());
            LOG(WARNING, "[MetaCheck] Cannot find store_path %s", path.c_str());
            continue;
        }
        leveldb::DB* disk_db = db_it->second;
        disk_db->Put(leveldb::WriteOptions(), it->key(), it->value());
        if (path != src_meta_path) {
            src_meta->Delete(leveldb::WriteOptions(), it->key());
        }
    }
    for (auto it = meta_dbs.begin(); it != meta_dbs.end(); ++it) {
        leveldb::DB* ldb = it->second;
        ldb->Put(leveldb::WriteOptions(), version_key, version_str);
    }
    delete it;
}

void SetChunkserverMetaVersion(const std::map<std::string, leveldb::DB*>& meta_dbs) {
    for (auto it = meta_dbs.begin(); it != meta_dbs.end(); ++it) {
        leveldb::DB* ldb = it->second;
        std::string meta_key(8, '\0');
        meta_key.append("meta");
        std::string meta_str(4, '\0');
        *(reinterpret_cast<int*>(&meta_str[0])) = CHUNKSERVER_META_VERSION;
        leveldb::Status s = ldb->Put(leveldb::WriteOptions(), meta_key, meta_str);
        if (!s.ok()) {
            LOG(ERROR, "[MetaCheck] Put meta failed %s", it->first.c_str());
            exit(EXIT_FAILURE);
        }
        LOG(INFO, "[MetaCheck] Set meta version %s = %d", it->first.c_str(), CHUNKSERVER_META_VERSION);
    }
}

void CloseMetaStore(const std::map<std::string, leveldb::DB*>& meta_dbs) {
    for (auto it = meta_dbs.begin(); it != meta_dbs.end(); ++it) {
        delete it->second;
    }
}

} // namespace bfs
} // namespace baidu