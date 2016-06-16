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

class BlockMapping {
public:
    BlockMapping();
    bool GetBlock(int64_t block_id, NSBlock* block);
    bool GetLocatedBlock(int64_t id, std::vector<int32_t>* replica, int64_t* block_size);
    bool ChangeReplicaNum(int64_t block_id, int32_t replica_num);
    void AddNewBlock(int64_t block_id, int32_t replica,
                     int64_t version, int64_t block_size,
                     const std::vector<int32_t>* init_replicas);
    bool UpdateBlockInfo(int64_t block_id, int32_t server_id, int64_t block_size,
                         int64_t block_version);
    void RemoveBlocksForFile(const FileInfo& file_info);
    void RemoveBlock(int64_t block_id);
    void DealWithDeadNode(int32_t cs_id, const std::set<int64_t>& blocks);
    StatusCode CheckBlockVersion(int64_t block_id, int64_t version);
    void PickRecoverBlocks(int32_t cs_id, int32_t block_num,
                           std::map<int64_t, std::set<int32_t> >* recover_blocks,
                           RecoverPri pri);
    void ProcessRecoveredBlock(int32_t cs_id, int64_t block_id);
    void GetCloseBlocks(int32_t cs_id, google::protobuf::RepeatedField<int64_t>* close_blocks);
    void GetStat(int64_t* lo_recover_num, int64_t* hi_recover_num,
                 int64_t* lo_pending, int64_t* hi_pending,
                 int64_t* lost_num, int64_t* incomplete_num);
    void ListRecover(std::set<int64_t>* hi_recover,
                     std::set<int64_t>* lo_recover,
                     std::set<int64_t>* lost,
                     std::map<int32_t, std::set<int64_t> >* hi_check,
                     std::map<int32_t, std::set<int64_t> >* lo_check,
                     std::map<int32_t, std::set<int64_t> >* incomplete,
                     int32_t upbound_size);
    void SetSafeMode(bool safe_mode);
    int32_t GetCheckNum();
    void MarkIncomplete(int64_t block_id);
private:
    void DealWithDeadBlock(int32_t cs_id, int64_t block_id);
    typedef std::map<int32_t, std::set<int64_t> > CheckList;
    void ListCheckList(const CheckList& check_list, std::map<int32_t, std::set<int64_t> >* result);
    void PickRecoverFromSet(int32_t cs_id, int32_t quota, std::set<int64_t>* recover_set,
                            std::map<int64_t, std::set<int32_t> >* recover_blocks,
                            std::set<int64_t>* check_set);
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
    ThreadPool thread_pool_;
    typedef std::map<int64_t, NSBlock*> NSBlockMap;
    NSBlockMap block_map_;
    int64_t next_block_id_;
    bool safe_mode_;

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
