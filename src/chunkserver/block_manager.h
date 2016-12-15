// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//

#ifndef  BAIDU_BFS_BLOCK_MANAGER_H_
#define  BAIDU_BFS_BLOCK_MANAGER_H_

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
class Disk;
typedef DiskCounterManager::DiskStat DiskStat;

class BlockManager {
public:
    BlockManager(const std::string& store_path);
    ~BlockManager();
    int64_t DiskQuota()  const;
    bool LoadStorage();
    int64_t NameSpaceVersion() const;
    bool SetNameSpaceVersion(int64_t version);
    int64_t ListBlocks(std::vector<BlockMeta>* blocks, int64_t offset, uint32_t num);
    Block* CreateBlock(int64_t block_id,  StatusCode* status);
    bool CloseBlock(Block* block);
    StatusCode RemoveBlock(int64_t block_id);
    bool CleanUp(int64_t namespace_version);

    Block* FindBlock(int64_t block_id);
    bool AddBlock(int64_t block_id, Disk* disk, BlockMeta meta);

    DiskStat Stat();

private:
    void CheckStorePath(const std::string& store_path);
    void LoadOneDisk(Disk* disk);
    Disk* PickDisk(int64_t block_id);
    int64_t FindSmallest(std::vector<leveldb::Iterator*>& iters, int32_t* idx);
    void LogStatus();
private:
    ThreadPool* thread_pool_;
    std::vector<std::pair<Disk*, DiskStat>> disks_;
    FileCache* file_cache_;
    Mutex   mu_;
    std::map<int64_t, Block*> block_map_;
    int64_t disk_quota_;
    DiskStat stat_;
    DiskCounterManager* counter_manager_;
};

} // bfs
} // baidu
#endif  // BAIDU_BFS_BLOCK_MANAGER_H_

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
