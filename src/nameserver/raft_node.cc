// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#include "nameserver/raft_node.h"

#include <boost/bind.hpp>
#include <gflags/gflags.h>
#include <common/mutex.h>
#include <common/logging.h>
#include <common/string_util.h>

#include "rpc/rpc_client.h"

DECLARE_string(raftdb_path);
DECLARE_string(raft_nodes);
DECLARE_int32(raft_node_index);

namespace baidu {
namespace bfs {

RaftNodeImpl::RaftNodeImpl()
    : current_term_(0), log_index_(0), commit_index_(0),
      last_applied_(0), node_state_(kFollower) {
    common::SplitString(FLAGS_raft_nodes, ",", &nodes_);
    uint32_t index = FLAGS_raft_node_index;
    if (nodes_.size() < 1U || nodes_.size() < index + 1) {
        LOG(FATAL, "Wrong flags raft_nodes: %s %ld", FLAGS_raft_nodes.c_str(),
            FLAGS_raft_node_index);
    }
    self_ = nodes_[index];
    leveldb::Options options;
    options.create_if_missing = true;
    leveldb::Status s = leveldb::DB::Open(options, FLAGS_raftdb_path, &log_db_);
    if (!s.ok()) {
        log_db_ = NULL;
        LOG(FATAL, "Open raft db fail: %s\n", s.ToString().c_str());
    }
    rpc_client_ = new RpcClient();
    election_taskid_ = 
        thread_pool_.DelayTask(150 + rand() % 150, boost::bind(&RaftNodeImpl::Election, this));
}

RaftNodeImpl::~RaftNodeImpl() {
}

void RaftNodeImpl::Election() {
    MutexLock lock(&mu_);
    if (node_state_ == kLeader) {
        return;
    }

    current_term_ ++;
    node_state_ = kCandidate;
    voted_for_ = self_;
    
    for (uint32_t i = 0; i < nodes_.size(); i++) {
        VoteRequest* request = new VoteRequest;
        request->set_term(current_term_);
        request->set_candidate(self_);
        VoteResponse* response = new VoteResponse;
        RaftNode_Stub* raft_node;
        rpc_client_->GetStub(nodes_[i], &raft_node);
        boost::function<void (const VoteRequest*, VoteResponse*, bool, int)> callback
                = boost::bind(&RaftNodeImpl::ElectionCallback, this, _1, _2, _3, _4, nodes_[i]);
        rpc_client_->AsyncRequest(raft_node, &RaftNode_Stub::Vote, request, response, callback, 60, 1);
        delete raft_node;
    }
    election_taskid_ = 
        thread_pool_.DelayTask(150 + rand() % 150, boost::bind(&RaftNodeImpl::Election, this));
}

void RaftNodeImpl::ElectionCallback(const VoteRequest* request,
                                    VoteResponse* response,
                                    bool failed,
                                    int error,
                                    const std::string& node_addr) {
    MutexLock lock(&mu_);
    if (!failed) {
        if (response->vote_granted() && request->term() == current_term_) {
            voted_.insert(node_addr);
            if (voted_.size() > (nodes_.size() / 2) + 1) {
                leader_ = "self";
                node_state_ = kLeader;
            }
        } else {
            if (response->term() > current_term_) {
                current_term_ = response->term();
            }
        }
    }
    delete request;
    delete response;
    if (node_state_ == kLeader) {
        /// send heartbeat
    }
}

void RaftNodeImpl::Vote(::google::protobuf::RpcController* controller,
                    const ::baidu::bfs::VoteRequest* request,
                    ::baidu::bfs::VoteResponse* response,
                    ::google::protobuf::Closure* done) {
    int64_t term = request->term();
    const std::string& candidate = request->candidate();
    int64_t last_log_index = request->last_log_index();

    MutexLock lock(&mu_);
    if (term < current_term_) {
        response->set_vote_granted(false);
        done->Run();
        return;
    }
    if (voted_for_ == "" || (candidate != "" && last_log_index >= log_index_)) {
        voted_for_ = candidate;
        response->set_vote_granted(true);
        done->Run();
        return;
    }
}

std::string RaftNodeImpl::GetLeader() {
    return "self";
}

void RaftNodeImpl::DoReplicateLog() {
    for (uint32_t i = 0; i < nodes_.size(); ++i) {
        AppendEntriesRequest* request = new AppendEntriesRequest;
        AppendEntriesResponse* response = new AppendEntriesResponse;
        request->set_term(current_term_);
        //request->set_leaderid
        RaftNode_Stub* node;
        rpc_client_->GetStub(nodes_[i], &node);
        rpc_client_->SendRequest(node, &RaftNode_Stub::AppendEntries, request, response, 60, 3);
        delete node;
    }
}

void RaftNodeImpl::AppendLog(const std::string& log, boost::function<void ()> callback) {
    /// append log to entries and wait for commit
    /*
    char idstr[32];
    snprintf(idstr, sizeof(idstr), "%20d", ++next_logid_);
    log_db_->Put(leveldb::WriteOptions(), leveldb::Slice(idstr, 20), log);
    DoReplicateLog();
    */
    return;
}

void RaftNodeImpl::AppendEntries(::google::protobuf::RpcController* controller,
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

}
}
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
