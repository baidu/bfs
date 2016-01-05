// Copyright (c) 2014, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <set>
#include <map>

#include <gflags/gflags.h>

#include <common/mutex.h>
#include "proto/nameserver.pb.h"

namespace baidu {
namespace bfs {

struct NSBlock {
    int64_t id;
    int64_t version;
    std::set<int32_t> replica;
    int64_t block_size;
    int32_t expect_replica_num;
    bool pending_change;
    std::set<int32_t> pulling_chunkservers;
    NSBlock(int64_t block_id);
};

class BlockMapping {
public:

    BlockMapping();
    int64_t NewBlockID();
    bool GetBlock(int64_t block_id, NSBlock* block);
    bool MarkBlockStable(int64_t block_id);
    bool GetReplicaLocation(int64_t id, std::set<int32_t>* chunkserver_id);
    void DealDeadBlocks(int32_t id, std::set<int64_t> blocks);
    bool ChangeReplicaNum(int64_t block_id, int32_t replica_num);
    void AddNewBlock(int64_t block_id);
    bool UpdateBlockInfo(int64_t id, int32_t server_id, int64_t block_size,
                         int64_t block_version, int32_t* more_replica_num = NULL);
    void RemoveBlocksForFile(const FileInfo& file_info);
    void RemoveBlock(int64_t block_id);
    bool MarkPullBlock(int32_t dst_cs, int64_t block_id);
    void UnmarkPullBlock(int32_t cs_id, int64_t block_id);
    bool GetPullBlocks(int32_t id, std::vector<std::pair<int64_t, std::set<int32_t> > >* blocks);
    bool SetBlockVersion(int64_t block_id, int64_t version);

private:
    Mutex _mu;
    typedef std::map<int64_t, NSBlock*> NSBlockMap;
    NSBlockMap _block_map;
    int64_t _next_block_id;
    std::map<int32_t, std::set<int64_t> > _blocks_to_replicate;
};

} // namespace bfs
} // namespace baidu
