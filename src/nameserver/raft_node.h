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
    void AppendLog(const std::string& log);
    void RegisterCallback(boost::function<void (const std::string& log)> callback);
private:
    bool CancelElection();
    void ResetElection();
    void DoReplicateLog();
    void ReplicateLogForNode(int id);
    void Election();
    bool CheckTerm(int64_t term);
    void ElectionCallback(const VoteRequest* request,
                          VoteResponse* response,
                          bool failed,
                          int error,
                          const std::string& node_addr);
    std::string LoadVoteFor();
    void SetVeteFor(const std::string& votefor);
    int64_t LoadCurrentTerm();
    void SetCurrentTerm(int64_t);
private:
    std::vector<std::string> nodes_;
    std::string self_;

    int64_t current_term_;
    std::string voted_for_;
    leveldb::DB* log_db_;
    int64_t log_index_; /// ????
    int64_t log_term_;

    int64_t commit_index_;
    int64_t last_applied_;

    std::vector<int64_t> next_index_;
    std::vector<int64_t> match_index_;

    Mutex mu_;
    common::ThreadPool*  thread_pool_;
    RpcClient*   rpc_client_;
    std::set<std::string> voted_;   /// À≠Õ∂Œ“¡À
    std::string leader_;
    int64_t election_taskid_;

    boost::function<void (const std::string& log)> log_callback_;
    NodeState node_state_;
};

}
}

#endif  // BFS_RAFT_NODE_H_

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
