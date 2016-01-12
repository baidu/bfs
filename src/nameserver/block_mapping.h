// Copyright (c) 2014, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <set>
#include <map>
#include <queue>

#include <gflags/gflags.h>

#include <common/mutex.h>
#include <common/thread_pool.h>
#include "proto/nameserver.pb.h"

namespace baidu {
namespace bfs {

struct NSBlock {
    int64_t id;
    int64_t version;
    std::set<int32_t> replica;
    int64_t block_size;
    int32_t expect_replica_num;
    bool pending_recover;
    NSBlock(int64_t block_id);
    bool operator<(const NSBlock &b) const {
        return (this->replica.size() >= b.replica.size());
    }
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
                         int64_t block_version, bool need_recovery);
    void RemoveBlocksForFile(const FileInfo& file_info);
    void RemoveBlock(int64_t block_id);
    void DealWithDeadBlocks(int64_t cs_id, std::set<int64_t>& blocks);
    bool SetBlockVersion(int64_t block_id, int64_t version);
    void PickRecoverBlocks(int64_t cs_id, int64_t block_num, std::map<int64_t, int64_t>* recover_blocks);
    void ProcessRecoveredBlock(int64_t cs_id, int64_t block_id);
    void GetStat(int64_t* recover_num, int64_t* pending_num);

private:
    void AddToRecover(NSBlock* nsblock);
    void CheckRecover(int64_t block_id);
    bool GetBlockPtr(int64_t block_id, NSBlock** block);

private:
    Mutex mu_;
    ThreadPool thread_pool_;
    typedef std::map<int64_t, NSBlock*> NSBlockMap;
    NSBlockMap block_map_;
    int64_t next_block_id_;

    std::priority_queue<std::pair<int64_t, int64_t> > recover_q_;
    std::set<int64_t> recover_check_;
};

} // namespace bfs
} // namespace baidu
