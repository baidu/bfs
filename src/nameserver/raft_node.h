// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#ifndef  BFS_RAFT_NODE_H_
#define  BFS_RAFT_NODE_H_

#include <stdint.h>

#include <string>
#include <vector>

#include <leveldb/db.h>
#include <common/mutex.h>
#include <common/thread.h>
#include <common/thread_pool.h>

#include "proto/raft.pb.h"

namespace baidu {
namespace bfs {

class RpcClient;

enum NodeState {
    kFollower = 0,
    kCandidate = 1,
    kLeader = 2,
};

class RaftNodeImpl : public RaftNode {
public:
    RaftNodeImpl();
    ~RaftNodeImpl();
    void Vote(::google::protobuf::RpcController* controller,
              const ::baidu::bfs::VoteRequest* request,
              ::baidu::bfs::VoteResponse* response,
              ::google::protobuf::Closure* done);
    void AppendEntries(::google::protobuf::RpcController* controller,
                       const ::baidu::bfs::AppendEntriesRequest* request,
                       ::baidu::bfs::AppendEntriesResponse* response,
                       ::google::protobuf::Closure* done);
public:
    bool GetLeader(std::string* leader);
    void AppendLog(const std::string& log, boost::function<void ()> callback);
    bool AppendLog(const std::string& log, int timeout_ms = 10000);
    void RegisterCallback(boost::function<void (const std::string& log)> callback);
private:
    std::string Index2Logkey(int64_t index);
    void LoadStorage();
    bool CancelElection();
    void ResetElection();
    void ReplicateLogForNode(uint32_t id);
    void ReplicateLogWorker(uint32_t id);
    void Election();
    bool CheckTerm(int64_t term);
    void ElectionCallback(const VoteRequest* request,
                          VoteResponse* response,
                          bool failed,
                          int error,
                          const std::string& node_addr);
    bool StoreLog(int64_t term, int64_t index, const std::string& log);
    void ApplyLog();

    std::string LoadVoteFor();
    void SetVeteFor(const std::string& votefor);
    int64_t LoadCurrentTerm();
    void SetCurrentTerm(int64_t);
    void SetLastApplied(int64_t index);
    int64_t GetLastApplied(int64_t index);
private:
    std::vector<std::string> nodes_;
    std::string self_;

    int64_t current_term_;      /// 当前term
    std::string voted_for_;     /// 当前term下投的票
    leveldb::DB* log_db_;       /// log持久存储
    int64_t log_index_;         /// 上一条log的index
    int64_t log_term_;          /// 上一条log的term

    int64_t commit_index_;      /// 提交的log的index
    int64_t last_applied_;      /// 应用到状态机的index
    bool applying_;             /// 正在提交到状态机

    bool node_stop_;
    struct FollowerContext {
        int64_t next_index;
        int64_t match_index;
        common::ThreadPool worker;
        common::CondVar condition;
        FollowerContext(Mutex* mu) : next_index(0), match_index(0), worker(1), condition(mu) {}
    };
    std::vector<FollowerContext*> follower_context_;

    Mutex mu_;
    common::ThreadPool*  thread_pool_;
    RpcClient*   rpc_client_;
    std::set<std::string> voted_;   /// 谁投我了
    std::string leader_;
    int64_t election_taskid_;

    boost::function<void (const std::string& log)> log_callback_;
    NodeState node_state_;
};

}
}

#endif  // BFS_RAFT_NODE_H_

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
