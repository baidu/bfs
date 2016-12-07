// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//

#ifndef  BAIDU_BFS_DISK_H_
#define  BAIDU_BFS_DISK_H_

#include <stdint.h>
#include <map>
#include <string>
#include <vector>

#include <common/thread_pool.h>
#include <common/counter.h>
#include "proto/status_code.pb.h"

namespace leveldb {
class DB;
class Iterator;
}

namespace baidu {
namespace bfs {

class BlockMeta;
class Block;
class FileCache;

struct DiskStats {
    common::Counter blocks;
    common::Counter data_size;
    common::Counter find_ops;
};

struct BlockStats {
    common::Counter writing_buffers;
};

class Disk {
public:
    Disk(const std::string& path, FileCache* cache, int64_t quota);
    ~Disk();
    std::string Path() const;
    bool LoadStorage(std::function<void (int64_t, Block*)> callback);
    int64_t NameSpaceVersion() const;
    bool SetNameSpaceVersion(int64_t version);
    void Seek(int64_t block_id, std::vector<leveldb::Iterator*>* iters);
    bool SyncBlockMeta(const BlockMeta& meta);
    bool RemoveBlockMeta(int64_t block_id);
    void AddTask(std::function<void ()> func, bool is_priority);

    bool CloseBlock(Block* block);
    bool RemoveBlock(int64_t block_id);
    bool CleanUp();
private:
    std::string BlockId2Str(int64_t block_id);
private:
    std::string path_;
    ThreadPool* thread_pool_;
    FileCache* file_cache_;
    int64_t disk_quota_;
    leveldb::DB* metadb_;
    Mutex   mu_;
    int64_t namespace_version_;
};

} // bfs
} // baidu
#endif  // BAIDU_BFS_DISK_H_

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
