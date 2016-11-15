// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#ifndef  BAIDU_BFS_BLOCK_MANAGER_H_
#define  BAIDU_BFS_BLOCK_MANAGER_H_

#include <stdint.h>
#include <map>
#include <string>
#include <vector>

#include <common/thread_pool.h>
#include "proto/status_code.pb.h"

namespace leveldb {
class DB;
}

namespace baidu {
namespace bfs {

class BlockMeta;
class Block;
class FileCache;

class BlockManager {
public:
    BlockManager(const std::string& store_path);
    ~BlockManager();
    int64_t DiskQuota()  const;
    /// Load meta from disk
    bool LoadStorage();
    int64_t NameSpaceVersion() const;
    bool SetNameSpaceVersion(int64_t version);
    int64_t ListBlocks(std::vector<BlockMeta>* blocks, int64_t offset, int32_t num);
    Block* CreateBlock(int64_t block_id, int64_t* sync_time, StatusCode* status);
    Block* FindBlock(int64_t block_id);
    bool CloseBlock(Block* block);
    bool RemoveBlock(int64_t block_id);
    bool RemoveAllBlocks();
private:
    std::string BlockId2Str(int64_t block_id);
    bool RemoveBlockMeta(int64_t block_id);
    bool RemoveAllBlocksAsync();
    void CheckStorePath(const std::string& store_path);
    const std::string& GetStorePath(int64_t block_id);
    bool SyncBlockMeta(const BlockMeta& meta, int64_t* sync_time);
private:
    ThreadPool* thread_pool_;
    std::vector<std::string> store_path_list_;
    typedef std::map<int64_t, Block*> BlockMap;
    BlockMap  block_map_;
    leveldb::DB* metadb_;
    FileCache* file_cache_;
    Mutex   mu_;
    int64_t namespace_version_;
    int64_t disk_quota_;
};

} // bfs
} // baidu
#endif  // BAIDU_BFS_BLOCK_MANAGER_H_

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
