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

#include "proto/nameserver.pb.h"

namespace baidu {
namespace bfs {

class RaftNode {
public:
    RaftNode();
    ~RaftNode();
    void Vote(::google::protobuf::RpcController* controller,
              const ::baidu::bfs::VoteRequest* request,
              ::baidu::bfs::VoteResponse* response,
              ::google::protobuf::Closure* done);
    void AppendEntries(::google::protobuf::RpcController* controller,
                       const ::baidu::bfs::AppendEntriesRequest* request,
                       ::baidu::bfs::AppendEntriesResponse* response,
                       ::google::protobuf::Closure* done);
public:
    std::string GetLeader();
    void AppendLog(const std::string& log, boost::function<void ()> callback);
private:
    void Election();
    void ElectionCallback();
private:
    std::vector<std::string> nodes_;
    std::string self_;

    int64_t current_term_;
    std::string voted_for_;
    leveldb::DB* log_db_;
    int64_t log_index_; /// ????

    int64_t commit_index_;
    int64_t last_applied_;

    std::vector<int64_t> next_index_;
    std::vector<int64_t> match_index_;

    Mutex mu_;
    ThreadPool  thread_pool_;
    RpcClient*   rpc_client_;
    std::set<std::string> voted_;
    std::string leader_;
};

}
}

#endif  // BFS_RAFT_NODE_H_

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
