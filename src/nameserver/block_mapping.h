// Copyright (c) 2014, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <set>
#include <map>
#include <vector>

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
    NSBlock(int64_t block_id);
};

class BlockMapping {
public:

    BlockMapping();
    int64_t NewBlockID();
    bool GetBlock(int64_t block_id, NSBlock* block);
    bool GetReplicaLocation(int64_t id, std::set<int32_t>* chunkserver_id);
    bool ChangeReplicaNum(int64_t block_id, int32_t replica_num);
    void AddNewBlock(int64_t block_id);
    bool UpdateBlockInfo(int64_t id, int32_t server_id, int64_t block_size,
                         int64_t block_version);
    void RemoveBlocksForFile(const FileInfo& file_info);
    void RemoveBlock(int64_t block_id);
    bool SetBlockVersion(int64_t block_id, int64_t version);
    void PickRecoverBlocks(int64_t cs_id, int64_t block_num, std::map<int64_t, int64_t>* recover_blocks);

private:
    void AddToRecover(NSBlock* nsblock, bool need_check);
    bool NeedRecover(NSBlock* nsblock);
    void CheckRecover(int64_t block_id, int64_t cs_id);

private:
    Mutex mu_;
    typedef std::map<int64_t, NSBlock*> NSBlockMap;
    NSBlockMap block_map_;
    int64_t next_block_id_;

    typedef std::map<int64_t, NSBlock*> RecoverList;
    RecoverList recover_hi_;
    RecoverList recover_lo_;
    typedef std::multimap<int64_t, int64_t> CheckList;
    CheckList recover_check_;
};

} // namespace bfs
} // namespace baidu
