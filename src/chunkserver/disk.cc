// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//

#include "chunkserver/disk.h"

#include <functional>
#include <sys/stat.h>

#include <gflags/gflags.h>
#include <leveldb/db.h>
#include <leveldb/cache.h>
#include <common/logging.h>
#include <common/string_util.h>

#include "chunkserver/data_block.h"

DECLARE_int32(chunkserver_io_thread_num);

namespace baidu {
namespace bfs {

extern common::Counter g_data_size;

Disk::Disk(const std::string& path, FileCache* cache, int64_t quota)
    : path_(path), file_cache_(cache), disk_quota_(quota) {
    thread_pool_ = new ThreadPool(FLAGS_chunkserver_io_thread_num);
}

Disk::~Disk() {
    thread_pool_->Stop(true);
    delete thread_pool_;
    delete metadb_;
    metadb_ = NULL;
}

bool Disk::LoadStorage(std::function<void (int64_t, Block*)> callback) {
    MutexLock lock(&mu_);
    int64_t start_load_time = common::timer::get_micros();
    leveldb::Options options;
    options.create_if_missing = true;
    leveldb::Status s = leveldb::DB::Open(options, path_ + "meta/", &metadb_);
    if (!s.ok()) {
        LOG(WARNING, "Load blocks fail: %s", s.ToString().c_str());
        return false;
    }

    std::string version_key(8, '\0');
    version_key.append("version");
    std::string version_str;
    s = metadb_->Get(leveldb::ReadOptions(), version_key, &version_str);
    if (s.ok() && version_str.size() == 8) {
        namespace_version_ = *(reinterpret_cast<int64_t*>(&version_str[0]));
        LOG(INFO, "Load namespace %ld", namespace_version_);
    }
    int block_num = 0;
    leveldb::Iterator* it = metadb_->NewIterator(leveldb::ReadOptions());
    for (it->Seek(version_key+'\0'); it->Valid(); it->Next()) {
        int64_t block_id = 0;
        if (1 != sscanf(it->key().data(), "%ld", &block_id)) {
            LOG(WARNING, "Unknown key: %s\n", it->key().ToString().c_str());
            delete it;
            return false;
        }
        BlockMeta meta;
        if (!meta.ParseFromArray(it->value().data(), it->value().size())) {
            LOG(INFO, "Parse meta for #%ld failed", block_id);
            assert(0); // TODO: fault tolerant
        }
        // TODO: do not need store_path in meta any more
        std::string file_path = meta.store_path() + Block::BuildFilePath(block_id);
        if (meta.version() < 0) {
            LOG(INFO, "Incomplete block #%ld V%ld %ld, drop it",
                block_id, meta.version(), meta.block_size());
            metadb_->Delete(leveldb::WriteOptions(), it->key());
            remove(file_path.c_str());
            continue;
        } else {
            struct stat st;
            if (stat(file_path.c_str(), &st) ||
                st.st_size != meta.block_size() ||
                access(file_path.c_str(), R_OK)) {
                LOG(WARNING, "Corrupted block #%ld V%ld size %ld path %s can't access: %s'",
                    block_id, meta.version(), meta.block_size(), file_path.c_str(),
                    strerror(errno));
                metadb_->Delete(leveldb::WriteOptions(), it->key());
                remove(file_path.c_str());
                continue;
            } else {
                LOG(DEBUG, "Load #%ld V%ld size %ld path %s",
                    block_id, meta.version(), meta.block_size(), file_path.c_str());
            }
        }
        Block* block = new Block(meta, thread_pool_, file_cache_);
        block->AddRef();
        callback(block_id, block);
        block_num ++;
    }
    delete it;
    int64_t end_load_time = common::timer::get_micros();
    LOG(INFO, "Disk %s Load %ld blocks, use %ld ms, namespace version: %ld",
        path_.c_str(), block_num, (end_load_time - start_load_time) / 1000, namespace_version_);
    if (namespace_version_ == 0 && block_num > 0) {
        LOG(WARNING, "Namespace version lost!");
    }
    disk_quota_ += g_data_size.Get();
    return true;
}

std::string Disk::Path() const {
    return path_;
}

int64_t Disk::NameSpaceVersion() const {
    return namespace_version_;
}

bool Disk::SetNameSpaceVersion(int64_t version) {
    MutexLock lock(&mu_);
    std::string version_key(8, '\0');
    version_key.append("version");
    std::string version_str(8, '\0');
    *(reinterpret_cast<int64_t*>(&version_str[0])) = version;
    leveldb::Status s = metadb_->Put(leveldb::WriteOptions(), version_key, version_str);
    if (!s.ok()) {
        LOG(WARNING, "SetNameSpaceVersion fail: %s", s.ToString().c_str());
        return false;
    }
    namespace_version_ = version;
    LOG(INFO, "Disk %s Set namespace version: %ld", path_.c_str(), namespace_version_);
    return true;
}

void Disk::Seek(int64_t block_id, std::vector<leveldb::Iterator*>* iters) {
    leveldb::Iterator* it = metadb_->NewIterator(leveldb::ReadOptions());
    it->Seek(BlockId2Str(block_id));
    if (it->Valid()) {
        int64_t id;
        if (1 == sscanf(it->key().data(), "%ld", &id)) {
            iters->push_back(it);
            return;
        } else {
            LOG(WARNING, "[ListBlocks] Unknown meta key: %s\n",
                    it->key().ToString().c_str());
        }
    }
    delete it;
}

bool Disk::SyncBlockMeta(const BlockMeta& meta) {
    std::string idstr = BlockId2Str(meta.block_id());
    leveldb::WriteOptions options;
    // options.sync = true;
    std::string meta_buf;
    meta.SerializeToString(&meta_buf);
    leveldb::Status s = metadb_->Put(options, idstr, meta_buf);
    if (!s.ok()) {
        Log(WARNING, "Write to meta fail:%s", idstr.c_str());
        return false;
    }
    return true;
}

bool Disk::RemoveBlockMeta(int64_t block_id) {
    std::string idstr = BlockId2Str(block_id);
    leveldb::Status s = metadb_->Delete(leveldb::WriteOptions(), idstr);
    if (!s.ok()) {
        LOG(WARNING, "Remove #%ld meta info fails: %s", block_id, s.ToString().c_str());
        return false;
    }
    return true;
}

bool Disk::CleanUp() {
    leveldb::Iterator* it = metadb_->NewIterator(leveldb::ReadOptions());
    for (it->Seek(BlockId2Str(0)); it->Valid(); it->Next()) {
        int64_t block_id = 0;
        if (1 != sscanf(it->key().data(), "%ld", &block_id)) {
            LOG(FATAL, "[ListBlocks] Unknown meta key: %s\n",
                it->key().ToString().c_str());
            delete it;
            return false;
        }
        BlockMeta meta;
        if (!meta.ParseFromArray(it->value().data(), it->value().size())) {
            LOG(INFO, "Parse meta for #%ld failed", block_id);
            delete it;
            return false;
        }
        std::string file_path = meta.store_path() + Block::BuildFilePath(block_id);
        remove(file_path.c_str());
    }
    delete it;
    std::string meta_path = path_ + "meta/";
    delete metadb_;
    remove(meta_path.c_str());
    leveldb::Options options;
    options.create_if_missing = true;
    leveldb::Status s = leveldb::DB::Open(options, path_ + "meta/", &metadb_);
    return true;
}

std::string Disk::BlockId2Str(int64_t block_id) {
    char idstr[64];
    snprintf(idstr, sizeof(idstr), "%13ld", block_id);
    return std::string(idstr);
}

} // namespace bfs
} // namespace baidu
