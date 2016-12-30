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

void ChunkserverMetaV0(const std::map<std::string, leveldb::DB*>& meta_dbs) {
    LOG(INFO, "[MetaCheck] Start converting chunkserver meta from verion 0");
    leveldb::DB* src_meta = meta_dbs.begin()->second;

    std::string version_key(8, '\0');
    version_key.append("version");
    std::string version_str;
    leveldb::Status s = src_meta->Get(leveldb::ReadOptions(), version_key, &version_str);
    if (!s.ok()) {
        LOG(ERROR, "[MetaCheck] Read namespace failed %s", meta_dbs.begin()->first.c_str());
    }

    leveldb::Iterator* it = src_meta->NewIterator(leveldb::ReadOptions());
    for (it->Seek(std::string(8, '\0') + "version" + '\0'); it->Valid(); it->Next()) {
        BlockMeta meta;
        if (!meta.ParseFromArray(it->value().data(), it->value().size())) {
            LOG(ERROR, "[MetaCheck] Parse BlockMeta failed: key = %s", it->key().ToString().c_str());
        }
        std::string path = meta.store_path();
        auto db_it = meta_dbs.find(path);
        if (db_it == meta_dbs.end()) {
            LOG(WARNING, "[MetaCheck] Cannot find store_path %s", path.c_str());
            continue;
        }
        leveldb::DB* disk_db = db_it->second;
        disk_db->Put(leveldb::WriteOptions(), it->key(), it->value());
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
        }
        LOG(INFO, "[MetaCheck] Set meta version %s = %d", it->first.c_str(), CHUNKSERVER_META_VERSION);
        std::string v;
        ldb->Get(leveldb::ReadOptions(), meta_key, &v);
        int mv = *(reinterpret_cast<int*>(&v[0]));
        LOG(INFO, "LL: after put %d", mv);
    }
}

void CleanUp(const std::map<std::string, leveldb::DB*>& meta_dbs) {
    for (auto it = meta_dbs.begin(); it != meta_dbs.end(); ++it) {
        delete it->second;
    }
}

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
            return;
        }

        std::string version_key(8, '\0');
        version_key.append("version");
        std::string version_str;
        s = metadb->Get(leveldb::ReadOptions(), version_key, &version_str);
        if (s.IsNotFound()) {
            LOG(INFO, "[MetaCheck] Load empty meta %s", path.c_str());
        } else {
            std::string meta_key(8, '\0');
            meta_key.append("meta");
            std::string meta_str;
            s = metadb->Get(leveldb::ReadOptions(), meta_key, &meta_str);
            if (s.ok()) {
                int m_version = *(reinterpret_cast<int*>(&meta_str[0]));
                LOG(INFO, "[MetaCheck] %s Load meta version %d", path.c_str(), m_version);
                if (meta_version != EMPTY_META && m_version != meta_version) {
                    LOG(ERROR, "Cannot handle this situation!!!");
                }
                meta_version = m_version;
            } else if (s.IsNotFound()) {
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
        SetChunkserverMetaVersion(meta_dbs);
    } else if (meta_version == 0) {
        ChunkserverMetaV0(meta_dbs);
        SetChunkserverMetaVersion(meta_dbs);
    } else {
        LOG(ERROR, "[MetaCheck] Cannot handle this situation!!!");
    }
    CleanUp(meta_dbs);
}

} // namespace bfs
} // namespace baidu