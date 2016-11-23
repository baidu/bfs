// Copyright (c) 2014, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef BFS_BLOCK_MAPPING_H_
#define BFS_BLOCK_MAPPING_H_

#include <set>
#include <map>
#include <queue>
#include <string>

#include <gflags/gflags.h>

#include <common/mutex.h>
#include <common/thread_pool.h>
#include "proto/status_code.pb.h"
#include "proto/nameserver.pb.h"

namespace baidu {
namespace bfs {

struct NSBlock {
    int64_t id;
    int64_t version;
    std::set<int32_t> replica;
    int64_t block_size;
    uint32_t expect_replica_num;
    RecoverStat recover_stat;
    std::set<int32_t> incomplete_replica;
    NSBlock();
    NSBlock(int64_t block_id, int32_t replica, int64_t version, int64_t size);
    bool operator<(const NSBlock &b) const {
        return (this->replica.size() >= b.replica.size());
    }
};

struct RecoverBlockNum {
    int64_t lo_recover_num;
    int64_t hi_recover_num;
    int64_t lo_pending;
    int64_t hi_pending;
    int64_t lost_num;
    int64_t incomplete_num;
    RecoverBlockNum() : lo_recover_num(0), hi_recover_num(0), lo_pending(0),
                        hi_pending(0), lost_num(0), incomplete_num(0) {}
};

struct RecoverBlockSet {
    std::set<int64_t> hi_recover;
    std::set<int64_t> lo_recover;
    std::set<int64_t> lost;
    std::map<int32_t, std::set<int64_t> > hi_check;
    std::map<int32_t, std::set<int64_t> > lo_check;
    std::map<int32_t, std::set<int64_t> > incomplete;
};

class BlockMapping {
public:
    BlockMapping(ThreadPool* thread_pool);
    bool GetBlock(int64_t block_id, NSBlock* block);
    bool GetLocatedBlock(int64_t id, std::vector<int32_t>* replica, int64_t* block_size, RecoverStat* status);
    bool ChangeReplicaNum(int64_t block_id, int32_t replica_num);
    void AddBlock(int64_t block_id, int32_t replica,
                  const std::vector<int32_t>& init_replicas);
    void RebuildBlock(int64_t block_id, int32_t replica,
                      int64_t version, int64_t size);
    bool UpdateBlockInfo(int64_t block_id, int32_t server_id, int64_t block_size,
                         int64_t block_version);
    void RemoveBlocksForFile(const FileInfo& file_info, std::map<int64_t, std::set<int32_t> >* blocks);
    void RemoveBlock(int64_t block_id, std::map<int64_t, std::set<int32_t> >* blocks);
    void DealWithDeadNode(int32_t cs_id, const std::set<int64_t>& blocks);
    void DealWithDeadBlock(int32_t cs_id, int64_t block_id);
    StatusCode CheckBlockVersion(int64_t block_id, int64_t version);
    void PickRecoverBlocks(int32_t cs_id, int32_t block_num,
                           std::vector<std::pair<int64_t, std::set<int32_t> > >* recover_blocks,
                           RecoverPri pri);
    void ProcessRecoveredBlock(int32_t cs_id, int64_t block_id, StatusCode status);
    void GetCloseBlocks(int32_t cs_id, google::protobuf::RepeatedField<int64_t>* close_blocks);
    void GetStat(int32_t cs_id, RecoverBlockNum* recover_num);
    void ListRecover(RecoverBlockSet* blocks);
    int32_t GetCheckNum();
    void MarkIncomplete(int64_t block_id);
private:
    void DealWithDeadBlockInternal(int32_t cs_id, int64_t block_id);
    typedef std::map<int32_t, std::set<int64_t> > CheckList;
    void ListCheckList(const CheckList& check_list, std::map<int32_t, std::set<int64_t> >* result);
    void ListRecoverList(const std::set<int64_t>& recover_set, std::set<int64_t>* result);
    void TryRecover(NSBlock* block);
    bool RemoveFromRecoverCheckList(int32_t cs_id, int64_t block_id);
    void CheckRecover(int32_t cs_id, int64_t block_id);
    void InsertToIncomplete(int64_t block_id, const std::set<int32_t>& inc_replica);
    void RemoveFromIncomplete(int64_t block_id, int32_t cs_id);
    bool GetBlockPtr(int64_t block_id, NSBlock** block);
    void SetState(NSBlock* block, RecoverStat stat);
    bool SetStateIf(NSBlock* block, RecoverStat from, RecoverStat to);

    bool UpdateWritingBlock(NSBlock* nsblock, int32_t cs_id, int64_t block_size,
                            int64_t block_version);
    bool UpdateNormalBlock(NSBlock* nsblock, int32_t cs_id, int64_t block_size,
                           int64_t block_version);
    bool UpdateIncompleteBlock(NSBlock* nsblock,int32_t cs_id, int64_t block_size,
                               int64_t block_version);
private:
    Mutex mu_;
    ThreadPool* thread_pool_;
    typedef std::map<int64_t, NSBlock*> NSBlockMap;
    NSBlockMap block_map_;

    CheckList hi_recover_check_;
    CheckList lo_recover_check_;
    CheckList incomplete_;
    std::set<int64_t> lo_pri_recover_;
    std::set<int64_t> hi_pri_recover_;
    std::set<int64_t> lost_blocks_;
};

} // namespace bfs
} // namespace baidu

#endif
