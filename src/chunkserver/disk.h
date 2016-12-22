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
#include "proto/status_code.pb.h"
#include "chunkserver/counter_manager.h"

namespace leveldb {
class DB;
class Iterator;
}

namespace baidu {
namespace bfs {

class BlockMeta;
class Block;
class FileCache;
typedef DiskCounterManager::DiskCounters DCounters;
typedef DiskCounterManager::DiskStat DiskStat;

class Disk {
public:
    Disk(const std::string& path, int64_t quota);
    ~Disk();
    std::string Path() const;
    bool LoadStorage(std::function<void (int64_t, Disk*, BlockMeta)> callback);
    int64_t NameSpaceVersion() const;
    bool SetNameSpaceVersion(int64_t version);
    void Seek(int64_t block_id, std::vector<leveldb::Iterator*>* iters);
    bool SyncBlockMeta(const BlockMeta& meta);
    bool RemoveBlockMeta(int64_t block_id);
    void AddTask(std::function<void ()> func, bool is_priority);
    int64_t GetQuota();
    double GetLoad();

    bool CloseBlock(Block* block);
    bool RemoveBlock(int64_t block_id);
    bool CleanUp();

    DiskStat Stat();
private:
    std::string BlockId2Str(int64_t block_id);
private:
    friend class Block;
    DCounters counters_;
    std::string path_;
    ThreadPool* thread_pool_;
    int64_t quota_;
    leveldb::DB* metadb_;
    Mutex   mu_;
    int64_t namespace_version_;
    DiskCounterManager counter_manager_;
};

} // bfs
} // baidu
#endif  // BAIDU_BFS_DISK_H_

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
