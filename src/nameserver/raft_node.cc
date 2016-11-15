// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#include "nameserver/raft_node.h"

#include <memory>

#include <gflags/gflags.h>
#include <common/mutex.h>
#include <common/logging.h>
#include <common/string_util.h>
#include <common/timer.h>

#include "rpc/rpc_client.h"

namespace baidu {
namespace bfs {

RaftNodeImpl::RaftNodeImpl(const std::string& raft_nodes,
                           int node_index,
                           const std::string& db_path)
    : current_term_(0), log_index_(0), log_term_(0), commit_index_(0),
      last_applied_(0), applying_(false), node_stop_(false), election_taskid_(-1),
      node_state_(kFollower) {
    common::SplitString(raft_nodes, ",", &nodes_);
    if (nodes_.size() < 1U || static_cast<int>(nodes_.size()) <= node_index) {
        LOG(FATAL, "Wrong flags raft_nodes: %s %d", raft_nodes.c_str(),
            node_index);
    }
    self_ = nodes_[node_index];

    LoadStorage(db_path);
    LOG(INFO, "Start RaftNode %s (%s)", self_.c_str(), raft_nodes.c_str());

    rpc_client_ = new RpcClient();
    srand(common::timer::get_micros());
    thread_pool_ = new common::ThreadPool();

    MutexLock lock(&mu_);
    ResetElection();
}

RaftNodeImpl::~RaftNodeImpl() {
    node_stop_ = true;
    delete thread_pool_;
    for (uint32_t i = 0; i < follower_context_.size(); i++) {
        if (follower_context_[i] == NULL) {
            continue;
        }
        follower_context_[i]->condition.Signal();
        follower_context_[i]->worker.Stop(true);
        delete follower_context_[i];
        follower_context_[i] = NULL;
    }
}

void RaftNodeImpl::LoadStorage(const std::string& db_path) {
    LogDB::Open(db_path, DBOption(), &log_db_);
    if (log_db_ == NULL) {
        LOG(FATAL, "Open logdb fail");
        return;
    }
    StatusCode s = log_db_->ReadMarker("last_applied", &last_applied_);
    if (s != kOK) {
        last_applied_ = 0;
    }
    s = log_db_->ReadMarker("current_term", &current_term_);
    if (s != kOK) {
        current_term_ = 0;
    }
    s = log_db_->ReadMarker("voted_for_", &voted_for_);
    if (s != kOK) {
        voted_for_ = "";
    }
    s = log_db_->GetLargestIdx(&log_index_);
    if (s != kOK) {
        log_index_ = 0;
    }
    LOG(INFO, "LoadStorage term %ld index %ld applied %ld",
        current_term_, log_index_, last_applied_);
    for (uint32_t i = 0; i < nodes_.size(); i++) {
        if (nodes_[i] == self_) {
            follower_context_.push_back(NULL);
            continue;
        }
        FollowerContext* ctx = new FollowerContext(&mu_);
        follower_context_.push_back(ctx);
        LOG(INFO, "New follower context %u %s", i, nodes_[i].c_str());
        ctx->next_index = log_index_ + 1;
        ctx->worker.AddTask(std::bind(&RaftNodeImpl::ReplicateLogWorker, this, i));
    }
}

void RaftNodeImpl::Election() {
    MutexLock lock(&mu_);
    if (node_state_ == kLeader) {
        election_taskid_ = -1;
        return;
    }

    voted_.clear();
    current_term_ ++;
    node_state_ = kCandidate;
    voted_for_ = self_;
    if (!StoreContext("current_term", current_term_) || !StoreContext("voted_for", voted_for_)) {
        LOG(FATAL, "Store term & vote_for fail %s %ld", voted_for_.c_str(), current_term_);
    }
    voted_.insert(self_);
    LOG(INFO, "Start Election: %ld %ld %ld", current_term_, log_index_, log_term_);
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
        std::function<void (const VoteRequest*, VoteResponse*, bool, int)> callback
                = std::bind(&RaftNodeImpl::ElectionCallback, this,
                            std::placeholders::_1, std::placeholders::_2,
                            std::placeholders::_3, std::placeholders::_4, nodes_[i]);
        rpc_client_->AsyncRequest(raft_node, &RaftNode_Stub::Vote, request, response, callback, 60, 1);
        delete raft_node;
    }
    election_taskid_ =
        thread_pool_->DelayTask(150 + rand() % 150, std::bind(&RaftNodeImpl::Election, this));
}

bool RaftNodeImpl::CheckTerm(int64_t term) {
    mu_.AssertHeld();
    if (term > current_term_) {
        if (node_state_ == kLeader) {
            LOG(FATAL, "Leader change to Follower, exit");
        } else {
            LOG(INFO, "Change state to Follower, reset election");
        }
        current_term_ = term;
        voted_for_ = "";
        if (!StoreContext("current_term", current_term_)
            || !StoreContext("voted_for", voted_for_)) {
            LOG(FATAL, "Store term & vote_for fail %s %ld",
                voted_for_.c_str(), current_term_);
        }
        node_state_ = kFollower;
        ResetElection();
        return false;
    }
    return true;
}

void RaftNodeImpl::ElectionCallback(const VoteRequest* request,
                                    VoteResponse* response,
                                    bool failed,
                                    int error,
                                    const std::string& node_addr) {
    std::unique_ptr<const VoteRequest> req(request);
    std::unique_ptr<VoteResponse> res(response);
    if (failed) {
        return;
    }

    int64_t term = response->term();
    bool granted = response->vote_granted();
    assert(term >= request->term() && (term == request->term() || !granted));

    LOG(INFO, "ElectionCallback %s by %s %ld / %ld",
        granted ? "granted" : "reject",
        node_addr.c_str(), term, current_term_);

    MutexLock lock(&mu_);
    CheckTerm(term);
    if (term != current_term_ || !granted || node_state_ == kLeader) {
        return;
    }

    voted_.insert(node_addr);
    if (voted_.size() >= (nodes_.size() / 2) + 1) {
        leader_ = self_;
        node_state_ = kLeader;
        CancelElection();
        LOG(INFO, "Change state to Leader, term %ld index %ld commit %ld applied %ld",
            current_term_, log_index_, commit_index_, last_applied_);
        StoreLog(current_term_, ++log_index_, "", kRaftCmd);
        for (uint32_t i = 0;i < follower_context_.size(); i++) {
            if (nodes_[i] != self_) {
                follower_context_[i]->match_index = 0;
                follower_context_[i]->next_index = log_index_ + 1;
                follower_context_[i]->condition.Signal();
            }
        }
    }
}

bool RaftNodeImpl::CancelElection() {
    mu_.AssertHeld();
    while (election_taskid_ != -1) {
        mu_.Unlock();
        ///TODO: race condition?
        //LOG(INFO, "Cancel election %ld", election_taskid_);
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
        thread_pool_->DelayTask(150 + rand() % 150, std::bind(&RaftNodeImpl::Election, this));
    //LOG(INFO, "Reset election %ld", election_taskid_);
}
void RaftNodeImpl::Vote(::google::protobuf::RpcController* controller,
                    const ::baidu::bfs::VoteRequest* request,
                    ::baidu::bfs::VoteResponse* response,
                    ::google::protobuf::Closure* done) {
    int64_t term = request->term();
    const std::string& candidate = request->candidate();
    int64_t last_log_index = request->last_log_index();
    int64_t last_log_term = request->last_log_term();
    LOG(INFO, "Recv vote request: %s %ld %ld / (%s %ld %ld)",
        candidate.c_str(), term, last_log_term,
        voted_for_.c_str(), current_term_, log_term_);
    MutexLock lock(&mu_);
    CheckTerm(term);
    if (term >= current_term_
        && (voted_for_ == "" || voted_for_ == candidate)
        && (last_log_term > log_term_ ||
        (last_log_term == log_term_ && last_log_index >= log_index_))) {
        voted_for_ = candidate;
        if (!StoreContext("current_term", current_term_) || !StoreContext("voted_for", voted_for_)) {
            LOG(FATAL, "Store term & vote_for fail %s %ld", voted_for_.c_str(), current_term_);
        } else {
            LOG(INFO, "Granted %s %ld %ld", candidate.c_str(), term, last_log_index);
        }
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
        if (leader_ != "" && leader_ != self_ && leader) {
            *leader = leader_;
        }
        return false;
    }
    if (last_applied_ < log_index_) {
        LOG(INFO, "Leader recover log %ld %ld %ld", log_index_, commit_index_, last_applied_);
        ApplyLog();
        return false;
    }
    *leader = self_;
    return true;
}

void RaftNodeImpl::ReplicateLogForNode(uint32_t id) {
    mu_.AssertHeld();

    FollowerContext* follower = follower_context_[id];
    int64_t next_index = follower->next_index;
    int64_t match_index = follower->match_index;

    mu_.Unlock();
    int64_t max_index = 0;
    int64_t max_term = -1;
    AppendEntriesRequest* request = new AppendEntriesRequest;
    AppendEntriesResponse* response = new AppendEntriesResponse;
    request->set_term(current_term_);
    request->set_leader(self_);
    request->set_leader_commit(commit_index_);
    LOG(INFO, "M %ld N %ld I %ld", match_index, next_index, log_index_);
    if (match_index < log_index_) {
        assert(match_index <= next_index);
        int64_t prev_index = 0;
        int64_t prev_term = 0;
        std::string prev_log;
        StatusCode s = log_db_->Read(next_index - 1, &prev_log);
        if (s == kOK) {
            LogEntry prev_entry;
            bool ret = prev_entry.ParseFromString(prev_log);
            assert(ret);
            prev_index = prev_entry.index();
            prev_term = prev_entry.term();
        }
        request->set_prev_log_index(prev_index);
        request->set_prev_log_term(prev_term);
        for (int64_t i = next_index; i <= log_index_; i++) {
            std::string log;
            s = log_db_->Read(i, &log);
            if (s != kOK) {
                LOG(FATAL, "Data lost: %ld", i);
                break;
            }
            LOG(INFO, "Add %ld to request", i);
            LogEntry* entry = request->add_entries();
            bool ret = entry->ParseFromString(log);
            assert(ret);
            max_index = entry->index();
            max_term = entry->term();
            if (request->ByteSize() >= 1024*1024) {
                break;
            }
        }
    }
    RaftNode_Stub* node;
    rpc_client_->GetStub(nodes_[id], &node);
    bool ret = rpc_client_->SendRequest(node, &RaftNode_Stub::AppendEntries,
                                        request, response, 1, 1);
    LOG(INFO, "Replicate %d entrys to %s return %d",
        request->entries_size(), nodes_[id].c_str(), ret);
    delete node;

    mu_.Lock();
    if (ret) {
        int64_t term = response->term();
        if (CheckTerm(term)) {
            if (response->success()) {
                if (max_index && max_term == current_term_) {
                    follower->match_index = max_index;
                    follower->next_index = max_index + 1;
                    LOG(INFO, "Replicate to %s success match %ld next %ld",
                        nodes_[id].c_str(), max_index, max_index + 1);
                    std::vector<int64_t> match_index;
                    for (uint32_t i = 0; i < nodes_.size(); i++) {
                        if (nodes_[i] == self_) {
                            match_index.push_back(1LL<<60);
                        } else {
                            match_index.push_back(follower_context_[i]->match_index);
                        }
                    }
                    std::sort(match_index.begin(), match_index.end());
                    int mid_pos = (nodes_.size() - 1) / 2;
                    int64_t commit_index = match_index[mid_pos];
                    //LOG(INFO, "match vector[ %ld %ld %ld ]",
                    //    match_index[0], match_index[1], match_index[2]);
                    assert(commit_index >= commit_index_);
                    if (commit_index > commit_index_) {
                        LOG(INFO, "Update commit_index from %ld to %ld",
                            commit_index_, commit_index);
                        commit_index_ = commit_index;
                        while (last_applied_ < commit_index) {
                            last_applied_ ++;
                            LOG(INFO, "[Raft] Apply %ld to leader", last_applied_);
                            std::map<int64_t, std::function<void (bool)> >::iterator cb_it =
                                callback_map_.find(last_applied_);
                            if (cb_it != callback_map_.end()) {
                                std::function<void (bool)> callback = cb_it->second;
                                callback_map_.erase(cb_it);
                                mu_.Unlock();
                                LOG(INFO, "[Raft] AppendLog callback %ld", last_applied_);
                                callback(true);
                                mu_.Lock();
                            } else {
                                LOG(INFO, "[Raft] no callback for %ld", last_applied_);
                            }
                        }
                        if (last_applied_ == commit_index) {
                            StoreContext("last_applied", last_applied_);
                        }
                    }
                }
            } else {
                LOG(INFO, "Replicate fail --next_index %ld for %s",
                    follower->next_index, nodes_[id].c_str());
                if (follower->next_index > follower->match_index
                    && follower->next_index > 1) {
                    --follower->next_index;
                }
            }
        }
    }
}

void RaftNodeImpl::ReplicateLogWorker(uint32_t id) {
    FollowerContext* follower = follower_context_[id];
    while (true) {
        MutexLock lock(&mu_);
        while (node_state_ != kLeader && !node_stop_) {
            follower->condition.Wait();
        }
        if (node_stop_) {
            return;
        }
        int64_t s = common::timer::get_micros();
        ReplicateLogForNode(id);
        int64_t d = 100 - (common::timer::get_micros() - s) / 1000;
        if (d > 0) {
            follower->condition.TimeWait(d);
        }
    }
}

std::string RaftNodeImpl::Index2Logkey(int64_t index) {
    char idstr[32];
    snprintf(idstr, sizeof(idstr), "%20ld", index);
    return std::string(idstr, 20);
}

bool RaftNodeImpl::StoreLog(int64_t term, int64_t index, const std::string& log, LogType type) {
    mu_.AssertHeld();
    LogEntry entry;
    entry.set_term(term);
    entry.set_index(index);
    entry.set_log_data(log);
    entry.set_type(type);
    std::string log_value;
    entry.SerializeToString(&log_value);
    StatusCode s = log_db_->Write(index, log_value);
    LOG(INFO, "Store %ld %ld %s to logdb return %s",
        term, index, common::DebugString(log).c_str(), StatusCode_Name(s).c_str());
    return s == kOK;
}

bool RaftNodeImpl::StoreContext(const std::string& context, int64_t value) {
    return StoreContext(context, std::string(reinterpret_cast<char*>(&value), sizeof(value)));
}

bool RaftNodeImpl::StoreContext(const std::string& context, const std::string& value) {
    LOG(INFO, "Store %s %s", context.c_str(), common::DebugString(value).c_str());
    StatusCode s = log_db_->WriteMarker(context, value);
    return s == kOK;
}

void RaftNodeImpl::AppendLog(const std::string& log, std::function<void (bool)> callback) {
    MutexLock lock(&mu_);
    int64_t index = ++log_index_;
    ///TODO: optimize lock
    if (!StoreLog(current_term_, index, log)) {
        log_index_ --;
        thread_pool_->AddTask(std::bind(callback,false));
        return;
    }
    callback_map_.insert(std::make_pair(index, callback));
    for (uint32_t i = 0; i < nodes_.size(); i++) {
        if (follower_context_[i]) {
            follower_context_[i]->condition.Signal();
        }
    }
}

bool RaftNodeImpl::AppendLog(const std::string& log, int timeout_ms) {
    int64_t index = 0;
    {
        MutexLock lock(&mu_);
        index = ++log_index_;
        ///TODO: optimize lock
        if (!StoreLog(current_term_, index, log)) {
            log_index_ --;
            return false;
        }
    }

    for (uint32_t i = 0; i < nodes_.size(); i++) {
        if (follower_context_[i]) {
            follower_context_[i]->condition.Signal();
        }
    }
    for (int i = 0; i < timeout_ms; i++) {
        usleep(1);
        if (commit_index_ >= index) {
            return true;
        }
    }
    LOG(WARNING, "AppendLog timeout %ld", index);
    return false;
}

void RaftNodeImpl::ApplyLog() {
    MutexLock lock(&mu_);
    if (applying_ || !log_callback_) {
        return;
    }
    applying_ = true;
    for (int64_t i = last_applied_ + 1; i <= commit_index_; i++) {
        std::string log;
        StatusCode s = log_db_->Read(i, &log);
        if (s != kOK) {
            LOG(FATAL, "Read logdb %ld fail", i);
        }
        LogEntry entry;
        bool ret = entry.ParseFromString(log);
        assert(ret);
        if (entry.type() == kUserLog) {
            mu_.Unlock();
            LOG(INFO, "Callback %ld %s",
                entry.index(), common::DebugString(entry.log_data()).c_str());
                log_callback_(entry.log_data());
            mu_.Lock();
        }
        last_applied_ = entry.index();
    }
    LOG(INFO, "Apply to %ld", last_applied_);
    StoreContext("last_applied", last_applied_);
    applying_ = false;
}

void RaftNodeImpl::AppendEntries(::google::protobuf::RpcController* controller,
                   const ::baidu::bfs::AppendEntriesRequest* request,
                   ::baidu::bfs::AppendEntriesResponse* response,
                   ::google::protobuf::Closure* done) {
    MutexLock lock(&mu_);
    int64_t term = request->term();
    if (term < current_term_) {
        LOG(INFO, "AppendEntries old term %ld / %ld", term, current_term_);
        response->set_success(false);
        done->Run();
        return;
    }

    CheckTerm(term);
    if (term == current_term_ && node_state_ == kCandidate) {
        node_state_ = kFollower;
    }
    leader_ = request->leader();
    ResetElection();
    int64_t prev_log_term = request->prev_log_term();
    int64_t prev_log_index = request->prev_log_index();
    if (prev_log_index > 0) {   // check prev term
        std::string log;
        StatusCode s = log_db_->Read(prev_log_index, &log);
        LogEntry entry;
        if (s == kOK && !entry.ParseFromString(log)) {
            LOG(FATAL, "Paser logdb value fail:%ld", prev_log_index);
        }
        if (s == kNsNotFound || entry.term() != prev_log_term) {
            LOG(INFO, "[Raft] Last index %ld term %ld / %ld mismatch",
                prev_log_index, prev_log_term, entry.term());
            response->set_success(false);
            done->Run();
            return;
        }
        LOG(INFO, "[Raft] Last index %ld term %ld match", prev_log_index, prev_log_term);
    }

    /// log match...
    int64_t leader_commit = request->leader_commit();
    int entry_count = request->entries_size();
    if (entry_count > 0) {
        LOG(INFO, "AppendEntries from %s %ld \"%s\" %ld",
            request->leader().c_str(), term,
            request->entries(0).log_data().c_str(), leader_commit);
        for (int i = 0; i < entry_count; i++) {
            int64_t index =request->entries(i).index();
            bool ret = StoreLog(current_term_, index,
                                request->entries(i).log_data());
            log_index_ = index;
            if (!ret) {
                response->set_success(false);
                done->Run();
                return;
            }
        }
    }

    response->set_term(current_term_);
    response->set_success(true);
    done->Run();

    if (leader_commit > commit_index_) {
        commit_index_ = leader_commit;
    }
    if (commit_index_ > last_applied_) {
        thread_pool_->AddTask(std::bind(&RaftNodeImpl::ApplyLog, this));
    }
}


void RaftNodeImpl::Init(std::function<void (const std::string& log)> callback) {
    log_callback_ = callback;
    ApplyLog();
}

}
}
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
