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
#include <common/timer.h>

#include "rpc/rpc_client.h"

DECLARE_string(raftdb_path);
DECLARE_string(raft_nodes);
DECLARE_int32(raft_node_index);

namespace baidu {
namespace bfs {

RaftNodeImpl::RaftNodeImpl()
    : current_term_(0), log_index_(0), log_term_(0), commit_index_(0),
      last_applied_(0), election_taskid_(-1), node_state_(kFollower) {
    common::SplitString(FLAGS_raft_nodes, ",", &nodes_);
    uint32_t index = FLAGS_raft_node_index;
    if (nodes_.size() < 1U || nodes_.size() <= index) {
        LOG(FATAL, "Wrong flags raft_nodes: %s %ld", FLAGS_raft_nodes.c_str(),
            FLAGS_raft_node_index);
    }
    self_ = nodes_[index];
    LOG(INFO, "Start RaftNode %s (%s)", self_.c_str(), FLAGS_raft_nodes.c_str());
    leveldb::Options options;
    options.create_if_missing = true;
    leveldb::Status s = leveldb::DB::Open(options, FLAGS_raftdb_path, &log_db_);
    if (!s.ok()) {
        log_db_ = NULL;
        LOG(FATAL, "Open raft db fail: %s\n", s.ToString().c_str());
    }
    rpc_client_ = new RpcClient();
    srand(common::timer::get_micros());
    thread_pool_ = new common::ThreadPool();
    MutexLock lock(&mu_);
    ResetElection();
}

RaftNodeImpl::~RaftNodeImpl() {
    delete thread_pool_;
}

void RaftNodeImpl::Election() {
    MutexLock lock(&mu_);
    if (node_state_ == kLeader) {
        election_taskid_ = -1;
        return;
    }

    current_term_ ++;
    LOG(INFO, "Start Election: %d %ld %ld", current_term_, log_index_, log_term_);
    node_state_ = kCandidate;
    voted_for_ = self_;
    voted_.insert(self_);
    
    for (uint32_t i = 0; i < nodes_.size(); i++) {
        if (nodes_[i] == self_) {
            continue;
        }
        LOG(INFO, "Send VoteRequest to %s", nodes_[i].c_str());
        VoteRequest* request = new VoteRequest;
        request->set_term(current_term_);
        request->set_candidate(self_);
        request->set_last_log_index(log_index_);
        request->set_last_log_term(log_term_);
        VoteResponse* response = new VoteResponse;
        RaftNode_Stub* raft_node;
        rpc_client_->GetStub(nodes_[i], &raft_node);
        boost::function<void (const VoteRequest*, VoteResponse*, bool, int)> callback
                = boost::bind(&RaftNodeImpl::ElectionCallback, this, _1, _2, _3, _4, nodes_[i]);
        rpc_client_->AsyncRequest(raft_node, &RaftNode_Stub::Vote, request, response, callback, 60, 1);
        delete raft_node;
    }
    election_taskid_ = 
        thread_pool_->DelayTask(150 + rand() % 150, boost::bind(&RaftNodeImpl::Election, this));
}

bool RaftNodeImpl::CheckTerm(int64_t term) {
    mu_.AssertHeld();
    if (term > current_term_) {
        current_term_ = term;
        voted_for_ = "";
        node_state_ = kFollower;
        ResetElection();
        LOG(INFO, "Change state to Follower, reset election");
        return false;
    }
    return true;
}

void RaftNodeImpl::ElectionCallback(const VoteRequest* request,
                                    VoteResponse* response,
                                    bool failed,
                                    int error,
                                    const std::string& node_addr) {
    MutexLock lock(&mu_);
    if (!failed
        && CheckTerm(response->term())
        && node_state_ != kLeader) {
        bool granted = response->vote_granted();
        int64_t term = response->term();
        LOG(INFO, "ElectionCallback granted %d by %s %ld / %ld",
            granted, node_addr.c_str(), term, current_term_);
        if (granted) {
            if (term == current_term_) {
                voted_.insert(node_addr);
                if (voted_.size() >= (nodes_.size() / 2) + 1) {
                    leader_ = self_;
                    node_state_ = kLeader;
                    bool ret = CancelElection();
                    LOG(INFO, "Change state to Leader, cancel election return %d", ret);
                    thread_pool_->AddTask(boost::bind(&RaftNodeImpl::DoReplicateLog, this));
                }
            } else {
                LOG(INFO, "Term mismatch %ld / %ld", term, current_term_);
            }
        }
    }
    delete request;
    delete response;
}

bool RaftNodeImpl::CancelElection() {
    mu_.AssertHeld();
    while (election_taskid_ != -1) {
        mu_.Unlock();
        LOG(INFO, "Cancel election %ld", election_taskid_);
        bool ret = thread_pool_->CancelTask(election_taskid_);
        mu_.Lock();
        if (ret) {
            election_taskid_ = -1;
        }
    }
    return true;
}

void RaftNodeImpl::ResetElection() {
    mu_.AssertHeld();
    if (election_taskid_ != -1) {
        CancelElection();
    }
    election_taskid_ = 
        thread_pool_->DelayTask(150 + rand() % 150, boost::bind(&RaftNodeImpl::Election, this));
    LOG(INFO, "Reset election %ld", election_taskid_);
}
void RaftNodeImpl::Vote(::google::protobuf::RpcController* controller,
                    const ::baidu::bfs::VoteRequest* request,
                    ::baidu::bfs::VoteResponse* response,
                    ::google::protobuf::Closure* done) {
    int64_t term = request->term();
    const std::string& candidate = request->candidate();
    int64_t last_log_index = request->last_log_index();
    int64_t last_log_term = request->last_log_term();
    LOG(INFO, "Recv vote request: %s %ld / %ld", candidate.c_str(), term, current_term_);
    MutexLock lock(&mu_);
    CheckTerm(term);
    if (term >= current_term_
        && (voted_for_ == "" || voted_for_ == candidate)
        && (last_log_term > log_term_ || 
        (last_log_term == log_term_ && last_log_index >= log_index_))) {
        voted_for_ = candidate;
        LOG(INFO, "Granted %s %ld %ld", candidate.c_str(), term, last_log_index);
        response->set_vote_granted(true);
        response->set_term(term);
        done->Run();
        return;
    }

    response->set_vote_granted(false);
    response->set_term(current_term_);
    done->Run();
}

bool RaftNodeImpl::GetLeader(std::string* leader) {
    if (leader == NULL || node_state_ != kLeader) {
        return false;
    }
    *leader = self_;
    return true;
}

void RaftNodeImpl::ReplicateLogForNode(int id) {
    AppendEntriesRequest* request = new AppendEntriesRequest;
    AppendEntriesResponse* response = new AppendEntriesResponse;
    request->set_term(current_term_);
    request->set_leader(self_);
    RaftNode_Stub* node;
    rpc_client_->GetStub(nodes_[id], &node);
    bool ret = rpc_client_->SendRequest(node, &RaftNode_Stub::AppendEntries, request, response, 1, 1);
    LOG(INFO, "Replicate to %s return %d", nodes_[id].c_str(), ret);
    delete node;
}

void RaftNodeImpl::DoReplicateLog() {
    MutexLock lock(&mu_);
    if (node_state_ != kLeader) {
        ResetElection();
        LOG(INFO, "Not leader, cancel ReplicateLog");
        return;
    }

    for (uint32_t i = 0; i < nodes_.size(); ++i) {
        if (nodes_[i] != self_) {
            thread_pool_->AddTask(boost::bind(&RaftNodeImpl::ReplicateLogForNode, this, i));
        }
    }
    thread_pool_->DelayTask(100, boost::bind(&RaftNodeImpl::DoReplicateLog, this));
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
    int64_t term = request->term();
    CheckTerm(term);
    if (term < current_term_) {
        response->set_success(false);
        done->Run();
    }
    int64_t leader_commit = request->leader_commit();
    /// log match...
    LOG(INFO, "AppendEntries from %s %ld %d %ld",
        request->leader().c_str(), term, request->entries_size(), leader_commit);
    response->set_success(true);
    ResetElection();
    done->Run();
}


void RaftNodeImpl::RegisterCallback(boost::function<void (const std::string& log)> callback) {
    log_callback_ = callback;
}

}
}
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
