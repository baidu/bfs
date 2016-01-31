// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#include "nameserver/raft_node.h"

#include <boost/bind.hpp>
#include <gflags/gflags.h>
#include <common/logging.h>
#include <common/string_util.h>

#include "rpc/rpc_client.h"

DECLARE_string(raftdb_path);
DECLARE_string(raft_nodes);
DECLARE_int32(raft_node_index);

namespace baidu {
namespace bfs {

RaftNode::RaftNode()
    : current_term_(0), log_index_(0), commit_index_(0), last_applied_(0) {
    common::SplitString(FLAGS_raft_nodes, ",", &nodes_);
    if (nodes_.size() < 1 || nodes_.size() < FLAGS_raft_node_index + 1) {
        LOG(FATAL, "No raft nodes: %s", FLAGS_raft_nodes.c_str());
    }
    self_ = nodes_[FLAGS_raft_node_index];
    leveldb::Options options;
    options.create_if_missing = true;
    leveldb::Status s = leveldb::DB::Open(options, FLAGS_raftdb_path, &log_db_);
    if (!s.ok()) {
        log_db_ = NULL;
        LOG(FATAL, "Open raft db fail: %s\n", s.ToString().c_str());
    }
    rpc_client_ = new RpcClient();
    thread_pool_->DelayTask(150 + rand() % 150, boost::bind(RaftNode::Election, this));
}
RaftNode::~RaftNode() {
}

void RaftNode::Election() {
    MutexLock lock(&mu_);
    if (leader_ == "self") {
        return;
    }
    current_term_ ++;
    for (int i = 0; i < nodes_.size(); i++) {
        VoteRequest* request = new VoteRequest;
        request->set_term(current_term_);
        request->set_candidate(self_);
        VoteResponse* response = new VoteResponse;
        RaftNode_Stub* node;
        rpc_client_->GetStub(nodes_[i], &node);
        boost::function<void ()> callback = boost::bind(&ElectionCallback, this);
        boost::function<void (const VoteRequest*, VoteResponse*, bool, int)> callback
                = boost::bind(&RaftNode::ElectionCallback, this, _1, _2, _3, _4, nodes_[i]);
        rpc_client_->AsyncRequest(node, RaftNode::Vote, request, response, callback);
    }
    thread_pool_->DelayTask(150 + rand() % 150, boost::bind(RaftNode::Election, this));
}

void RaftNode::ElectionCallback(const ::baidu::bfs::VoteRequest* request,
                                ::baidu::bfs::VoteResponse* response,
                                bool failed,
                                int error,
                                const std::string& node_addr) {
    MutexLock lock(&mu_);
    if (!failed) {
        if (response->vote_granted() && request->term() == current_term_) {
            voted_.insert(node_addr);
            if (voted_.size() > (nodes_.size() / 2) + 1) {
                leader_ = "self";
            }
        } else {
            if (response->term() > current_term_) {
                current_term_ = response->term();
            }
        }
    }
    delete request;
    delete response;
}

void RaftNode::Vote(::google::protobuf::RpcController* controller,
                    const ::baidu::bfs::VoteRequest* request,
                    ::baidu::bfs::VoteResponse* response,
                    ::google::protobuf::Closure* done) {
    int64_t term = request->term();
    const std::string& candidate = request->candidate();
    int64_t last_log_index = request->last_log_index();

    Mutexlock lock(&mu_);
    if (term < current_term_) {
        response->set_vote_granted(false);
        done->Run();
        return;
    }
    if (voted_for_ == "" || (candidate != "" && last_log_index >= log_index_)) {
        voted_for_ = candidate;
        response->set_vote_grated(true);
        done->Run();
        return;
    }
}

void RaftNode::AppendEntries(::google::protobuf::RpcController* controller,
                   const ::baidu::bfs::AppendEntriesRequest* request,
                   ::baidu::bfs::AppendEntriesResponse* response,
                   ::google::protobuf::Closure* done) {
    MutexLock lock(&mu_);
    if (request->term() < current_term_) {
        response->set_success(false);
        done->Run();
    }
    /// log match...
    response->set_success(true);
    done->Run();
}

std::string RaftNode::GetLeader() {
    return "self";
}

void RaftNode::AppendLog(const std::string& log, boost::function<void ()> callback) {
    /// append log to entries and wait for commit
    return;
}
}
}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
