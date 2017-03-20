// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//

#include <sys/stat.h>
#include <common/string_util.h>
#include <common/logging.h>
#include <common/timer.h>
#include <gflags/gflags.h>

#include "nameserver/master_slave.h"
#include "proto/status_code.pb.h"
#include "rpc/rpc_client.h"

DECLARE_bool(bfs_bug_tolerant);
DECLARE_string(nameserver_nodes);
DECLARE_int32(node_index);
DECLARE_string(master_slave_role);
DECLARE_int64(master_slave_log_limit);
DECLARE_int32(master_log_gc_interval);
DECLARE_int32(logdb_log_size);
DECLARE_int32(log_replicate_timeout);
DECLARE_int32(log_batch_size);

namespace baidu {
namespace bfs {

const std::string kLogPrefix = "\033[32m[Sync]\033[0m";

MasterSlaveImpl::MasterSlaveImpl() : slave_stub_(NULL), exiting_(false), master_only_(false),
                                     cond_(&mu_), log_done_(&mu_), term_(0), current_idx_(-1),
                                     applied_idx_(-1), sync_idx_(-1), gc_idx_(-1),
                                     slave_snapshot_seq_(0) {
    std::vector<std::string> nodes;
    common::SplitString(FLAGS_nameserver_nodes, ",", &nodes);
    std::string this_server = nodes[FLAGS_node_index];
    std::string another_server;
    if (FLAGS_node_index == 0) {
        another_server = nodes[1];
    } else if (FLAGS_node_index == 1) {
        another_server = nodes[0];
    } else {
        LOG(FATAL, "%s Nameserver does not belong to this cluster", kLogPrefix.c_str());
    }
    if (FLAGS_master_slave_role == "master") {
        master_addr_ = this_server;
        slave_addr_ = another_server;
        is_leader_ = true;
    } else if (FLAGS_master_slave_role == "slave") {
        master_addr_ = another_server;
        slave_addr_ = this_server;
        is_leader_ = false;
    } else {
        LOG(FATAL, "%s Wrong role: %s", kLogPrefix.c_str(),
            FLAGS_master_slave_role.c_str());
    }
    if (IsLeader()) {
        LOG(INFO, "%s I am Leader", kLogPrefix.c_str());
    } else {
        LOG(INFO, "%s I am Slave", kLogPrefix.c_str());
    }
    thread_pool_ = new common::ThreadPool(10);
    DBOption option;
    option.log_size = FLAGS_logdb_log_size;
    LogDB::Open("./logdb", option, &logdb_);
    if (logdb_ == NULL) {
        if (FLAGS_bfs_bug_tolerant) {
            CleanupLogdb();
        } else  {
            LOG(FATAL, "%s init logdb failed", kLogPrefix.c_str());
        }
    }
    if (IsLeader()) {
        thread_pool_->DelayTask(FLAGS_master_log_gc_interval * 1000,
                                std::bind(&MasterSlaveImpl::LogCleanUp, this));
    }
}

void MasterSlaveImpl::Init(SyncCallbacks callbacks) {
    log_callback_ = callbacks.log_callback;
    snapshot_callback_ = callbacks.snapshot_callback;
    erase_callback_ = callbacks.erase_callback;
    if (logdb_->GetLargestIdx(&current_idx_) == kReadError) {
        LOG(FATAL, "%s Read current_idx_ failed", kLogPrefix.c_str());
    }
    if (logdb_->ReadMarker("applied_idx", &applied_idx_) == kReadError) {
        LOG(FATAL, "%s ReadMarker applied_idx_ failed", kLogPrefix.c_str());
    }
    if (logdb_->ReadMarker("sync_idx", &sync_idx_) == kReadError) {
        LOG(FATAL, "%s ReadMarker sync_idx_ failed", kLogPrefix.c_str());
    }
    if (logdb_->ReadMarker("term", &term_) == kReadError) {
        LOG(FATAL, "%s ReadMarker term_ failed", kLogPrefix.c_str());
    }
    LOG(INFO, "%s set current_idx_ = %ld, applied_idx_ = %ld, sync_idx_ = %ld ",
        kLogPrefix.c_str(), current_idx_, applied_idx_, sync_idx_);
    assert(applied_idx_ <= current_idx_ && sync_idx_ <= current_idx_);
    while (IsLeader() && applied_idx_ < current_idx_) {
        std::string entry;
        StatusCode ret = logdb_->Read(applied_idx_ + 1, &entry);
        if (ret != kOK) {
            LOG(FATAL, "%s read logdb failed index %ld %s", kLogPrefix.c_str(),
                applied_idx_ + 1, StatusCode_Name(ret).c_str());
        }
        if (!entry.empty()) {
            log_callback_(entry);
        }
        applied_idx_++;
    }
    if (!IsLeader()) {
        // need to clear sync_idx and applied idx in case this is a retired master
        // which contains dirty data
        sync_idx_ = -1;
        applied_idx_ = -1;
    }

    rpc_client_ = new RpcClient();
    rpc_client_->GetStub(slave_addr_, &slave_stub_);
    if (IsLeader()) {
        worker_.Start(std::bind(&MasterSlaveImpl::BackgroundLog, this));
    }
    LogStatus();
}

bool MasterSlaveImpl::IsLeader(std::string* leader_addr) {
    return is_leader_;
}

////// Master //////
bool MasterSlaveImpl::Log(const std::string& entry, int timeout_ms) {
    if (!IsLeader()) {
        return true;
    }
    mu_.Lock();
    if (logdb_->Write(current_idx_ + 1, entry) != kOK) {
        LOG(FATAL, "%s write logdb failed index %ld",
            kLogPrefix.c_str(), current_idx_ + 1);
    }
    current_idx_++;
    applied_idx_ = current_idx_; // already updated namespace, applied_idx does not have actually meaning
    cond_.Signal();
    mu_.Unlock();
    // slave is way behind, do no wait
    if (master_only_ && sync_idx_ < current_idx_ - 1) {
        LOG(WARNING, "%s Sync in master-only mode, do not wait", kLogPrefix.c_str());
        return true;
    }

    int64_t start_point = common::timer::get_micros();
    int64_t stop_point = start_point + timeout_ms * 1000;
    while (sync_idx_ != current_idx_ && common::timer::get_micros() < stop_point) {
        int wait_time = (stop_point - common::timer::get_micros()) / 1000;
        MutexLock lock(&mu_);
        if (log_done_.TimeWait(wait_time)) {
            if (sync_idx_ != current_idx_) {
                continue;
            }
            if (master_only_) {
                LOG(INFO, "%s leaves master-only mode", kLogPrefix.c_str());
                master_only_ = false;
            }
            LOG(INFO, "%s sync log takes %ld ms", kLogPrefix.c_str(),
                (common::timer::get_micros() - start_point) / 1000);
            return true;
        } else {
            break;
        }
    }
    // log replicate time out
    LOG(WARNING, "%s Sync log timeout, Sync is in master-only mode", kLogPrefix.c_str());
    master_only_ = true;
    return true;
}

void MasterSlaveImpl::Log(const std::string& entry, std::function<void (bool)> callback) {
    if (!IsLeader()) {
        return;
    }
    MutexLock lock(&mu_);
    StatusCode s = logdb_->Write(current_idx_ + 1, entry);
    if (s != kOK) {
        if (s != kWriteError) {
            LOG(INFO, "%s write logdb failed index %ld reason %s",
                kLogPrefix.c_str(), current_idx_, StatusCode_Name(s).c_str());
        } else {
            LOG(FATAL, "%s write logdb failed index %ld ",
                kLogPrefix.c_str(), current_idx_ + 1);
        }
    }
    current_idx_++;
    applied_idx_ = current_idx_; // already updated namespace, applied_idx does not have actually meaning
    callbacks_.insert(std::make_pair(current_idx_, callback));
    if (master_only_ && sync_idx_ < current_idx_ - 1) { // slave is behind, do not wait
        thread_pool_->AddTask(std::bind(&MasterSlaveImpl::ProcessCallback,this,
                                        current_idx_, true));
    } else {
        LOG(DEBUG, "%s insert callback index = %d", kLogPrefix.c_str(), current_idx_);
        thread_pool_->DelayTask(FLAGS_log_replicate_timeout * 1000,
                                std::bind(&MasterSlaveImpl::ProcessCallback,
                                          this, current_idx_, true));
        cond_.Signal();
    }
    return;
}

void MasterSlaveImpl::SwitchToLeader() {
    MutexLock lock(&mu_);
    if (IsLeader()) {
        return;
    }
    sync_idx_ = -1;
    std::string old_master_addr = master_addr_;
    master_addr_ = slave_addr_;
    slave_addr_ = old_master_addr;
    rpc_client_->GetStub(slave_addr_, &slave_stub_);

    CleanupLogdb();
    gc_idx_ = current_idx_ - 1;
    is_leader_ = true;
    master_only_ = true;
    worker_.Start(std::bind(&MasterSlaveImpl::BackgroundLog, this));
    thread_pool_->DelayTask(FLAGS_master_log_gc_interval * 1000,
                            std::bind(&MasterSlaveImpl::LogCleanUp, this));
    LOG(INFO, "%s node switch to leader", kLogPrefix.c_str());
}

///    Slave    ///
void MasterSlaveImpl::AppendLog(::google::protobuf::RpcController* controller,
                                const master_slave::AppendLogRequest* request,
                                master_slave::AppendLogResponse* response,
                                ::google::protobuf::Closure* done) {
    if (IsLeader()) { // already switched to leader, does not accept new append entries
        response->set_success(false);
        done->Run();
        return;
    }
    if (request->index() == -1) {
        response->set_success(true);
        done->Run();
        return;
    }
    // term_ < request->term(): retired master brought up as a slave
    // term_ > reqeust->term(): maybe master lost it's logdb, or something else...
    if (term_ != request->term()) {
        LOG(INFO, "%s master term %ld slave term %ld, cleanup namespace",
            kLogPrefix.c_str(), request->term(), term_);
        erase_callback_();
        LOG(INFO, "%s cleanup namespace done", kLogPrefix.c_str());
        term_ = request->term();
        StatusCode s = logdb_->WriteMarker("term", term_);
        if (s != kOK) {
            LOG(FATAL, "%s Write marker term %ld failed", kLogPrefix.c_str(), term_);
        }
        response->set_index(0);
        response->set_success(false);
        done->Run();
        return;
    }
    // expect index to be current_idx_ + 1
    if (request->index() > current_idx_ + 1) {
        LOG(INFO, "%s out-date log request %ld, current_idx_ %ld",
            kLogPrefix.c_str(), request->index(), current_idx_);
        response->set_index(current_idx_ + 1);
        response->set_success(false);
        done->Run();
        return;
    } else if (request->index() <= current_idx_) {
        LOG(INFO, "%s out-date log request %ld, current_idx_ %ld",
            kLogPrefix.c_str(), request->index(), current_idx_);
        response->set_index(current_idx_ + 1);
        response->set_success(false);
        done->Run();
        return;
    }

    for (int32_t i = 0; i < request->log_data_size(); ++i) {
        log_callback_(request->log_data(i));
    }
    mu_.Lock();
    current_idx_ += request->log_data_size();
    mu_.Unlock();
    response->set_success(true);
    done->Run();
}

///    Slave    ///
void MasterSlaveImpl::Snapshot(::google::protobuf::RpcController* controller,
                                const master_slave::SnapshotRequest* request,
                                master_slave::SnapshotResponse* response,
                                ::google::protobuf::Closure* done) {
    if (IsLeader()) { // already switched to leader, does not accept snapshot
        LOG(WARNING, "%s Leader got a snapshot request", kLogPrefix.c_str());
        response->set_success(false);
        done->Run();
        return;
    }

    // term_ < request->term(): retired master brought up as a slave
    // term_ > reqeust->term(): maybe master lost it's logdb, or something else...
    if (term_ != request->term()) {
        LOG(INFO, "%s master term %ld slave term %ld, cleanup namespace",
            kLogPrefix.c_str(), request->term(), term_);
        erase_callback_();
        LOG(INFO, "%s cleanup namespace done", kLogPrefix.c_str());
        term_ = request->term();
        StatusCode s = logdb_->WriteMarker("term", term_);
        if (s != kOK) {
            LOG(FATAL, "%s Write marker term %ld failed", kLogPrefix.c_str(), term_);
        }
        response->set_success(false);
        done->Run();
        return;
    }

    int64_t seq = request->seq();
    LOG(INFO, "%s Got snapshot seq %ld", kLogPrefix.c_str(), seq);
    if (seq == 0) {
        LOG(INFO, "%s Start to clean up the old namespace...", kLogPrefix.c_str());
        erase_callback_();
        LOG(INFO, "%s Done clean up the old namespace...", kLogPrefix.c_str());
        term_ = request->term();
        StatusCode s = logdb_->WriteMarker("term", term_);
        if (s != kOK) {
            LOG(FATAL, "%s Write marker term %ld failed", kLogPrefix.c_str(), term_);
        }
        slave_snapshot_seq_ = 0;
    } else if (seq != slave_snapshot_seq_) {
        LOG(INFO, "%s snapshot mismatch reqeust seq = %ld, slave sseq = %ld",
            kLogPrefix.c_str(), seq, slave_snapshot_seq_);
        response->set_success(false);
        done->Run();
    }
    const std::string& data = request->data();
    if (data.empty()) {
        current_idx_ = request->index();
        response->set_success(true);
        LOG(INFO, "%s Receive snapshot complete seq = %ld", kLogPrefix.c_str(), seq);
        done->Run();
        return;
    }
    log_callback_(data);
    ++slave_snapshot_seq_;
    response->set_success(true);
    done->Run();
}

void MasterSlaveImpl::BackgroundLog() {
    while (true) {
        MutexLock lock(&mu_);
        while (!exiting_ && sync_idx_ == current_idx_) {
            LOG(DEBUG, "%s BackgroundLog waiting...", kLogPrefix.c_str());
            cond_.Wait();
        }
        if (exiting_) {
            return;
        }
        LOG(DEBUG, "%s BackgroundLog logging...", kLogPrefix.c_str());
        mu_.Unlock();
        ReplicateLog();
        mu_.Lock();
    }
}

void MasterSlaveImpl::ReplicateLog() {
    while (sync_idx_ < current_idx_) {
        MutexLock lock(&mu_);
        if (sync_idx_ == current_idx_) {
            break;
        }
        LOG(DEBUG, "%s ReplicateLog sync_idx_ = %d, current_idx_ = %d",
            kLogPrefix.c_str(), sync_idx_, current_idx_);

        if (sync_idx_ <= gc_idx_ && gc_idx_ != -1) {
            mu_.Unlock();
            if (!SendSnapshot()) {
                LOG(INFO, "%s Send snapshot failed", kLogPrefix.c_str());
                EmptyLog();
            }
            mu_.Lock();
            continue;
        }

        master_slave::AppendLogRequest request;
        master_slave::AppendLogResponse response;
        request.set_index(sync_idx_ + 1);
        request.set_term(term_);
        std::string entry;
        for (int i = 0; i < FLAGS_log_batch_size; ++i) {
            StatusCode s = logdb_->Read(sync_idx_ + 1 + i, &entry);
            if (s != kOK && s != kNsNotFound) {
                LOG(FATAL, "%s Read logdb_ failed sync_idx_ = %ld ",
                    kLogPrefix.c_str(), sync_idx_ + 1);
            }
            if (s == kNsNotFound) {
                break;
            }
            request.add_log_data(entry);
        }
        if (request.log_data_size() == 0) { // maybe slave is way behind
            continue;
        }
        mu_.Unlock();
        if (!rpc_client_->SendRequest(slave_stub_,
                                      &master_slave::MasterSlave_Stub::AppendLog,
                                      &request, &response, 15, 1)) {
            LOG(WARNING, "%s Replicate log failed index = %d, size = %d current_idx_ = %d",
                kLogPrefix.c_str(), sync_idx_ + 1, request.log_data_size(), current_idx_);
            EmptyLog();
            mu_.Lock();
            continue;
        }
        mu_.Lock();
        if (!response.success()) { // log mismatch
            sync_idx_ = response.index() - 1;
            LOG(INFO, "%s set sync_idx_ to %d", kLogPrefix.c_str(), sync_idx_);
            continue;
        }
        sync_idx_ = sync_idx_ + request.log_data_size();
        mu_.Unlock();
        for (int32_t i = 0; i < request.log_data_size(); ++i) {
            thread_pool_->AddTask(std::bind(&MasterSlaveImpl::ProcessCallback,
                                            this, sync_idx_ - i, false));
        }
        LOG(DEBUG, "%s Replicate log done. sync_idx_ = %d, size = %d current_idx_ = %d",
            kLogPrefix.c_str(), sync_idx_, request.log_data_size(), current_idx_);
        mu_.Lock();
    }
    log_done_.Signal();
}

void MasterSlaveImpl::EmptyLog() {
    LOG(INFO, "%s Sending empty log", kLogPrefix.c_str());
    master_slave::AppendLogRequest request;
    master_slave::AppendLogResponse response;
    request.set_index(-1);
    while (!rpc_client_->SendRequest(slave_stub_,
                                     &master_slave::MasterSlave_Stub::AppendLog,
                                     &request, &response, 15, 1)) {
        sleep(5);
    }
    LOG(INFO, "%s Slave re-connected", kLogPrefix.c_str());
}

bool MasterSlaveImpl::SendSnapshot() {
    int64_t current_index = current_idx_ - 1; // minus one to make sure slave does not get one entry short
    bool ret = false;
    int64_t seq = 0;
    LOG(INFO, "%s Start sending snapshot current_index_ %ld",
        kLogPrefix.c_str(), current_idx_);
    while (true) {
        std::string logstr;
        snapshot_callback_(0, &logstr);
        master_slave::SnapshotRequest request;
        master_slave::SnapshotResponse response;
        request.set_data(logstr);
        request.set_seq(seq);
        request.set_index(current_index);
        request.set_term(term_);
        for (int i = 0; i < 5; ++i) {
            if (rpc_client_->SendRequest(slave_stub_,
                                      &master_slave::MasterSlave_Stub::Snapshot,
                                      &request, &response, 15, 1)) {
                break;
            } else {
                LOG(WARNING, "%s Send snapshot failed seq %ld",
                    kLogPrefix.c_str(), seq);
                if (i == 4) {
                    snapshot_callback_(0, NULL);
                    return false;
                }
                sleep(5);
            }
        }
        LOG(INFO, "%s Send snapshot seq %ld done", kLogPrefix.c_str(), seq);
        if (!response.success()) {
            break;
        }
        if (logstr.empty()) {
            LOG(INFO, "%s Send snapshot complete seq %ld", kLogPrefix.c_str(), seq);
            sync_idx_ = current_index;
            ret = true;
            break;
        }
        ++seq;
    }
    snapshot_callback_(0, NULL);
    return ret;
}

void MasterSlaveImpl::ProcessCallback(int64_t index, bool timeout_check) {
    std::function<void (bool)> callback;
    MutexLock lock(&mu_);
    std::map<int64_t, std::function<void (bool)> >::iterator it = callbacks_.find(index);
    if (it != callbacks_.end()) {
        callback = it->second;
        LOG(DEBUG, "%s calling callback %d", kLogPrefix.c_str(), it->first);
        callbacks_.erase(it);
        mu_.Unlock();
        callback(true);
        mu_.Lock();
        if (timeout_check) {
            if (!master_only_) {
                LOG(WARNING, "%s ReplicateLog sync_idx_ = %d timeout, enter master-only mode",
                    kLogPrefix.c_str(), index);
            }
            master_only_ = true;
            return;
        }
    }
    if (master_only_ && index == current_idx_) {
        LOG(INFO, "%s leaves master-only mode", kLogPrefix.c_str());
        master_only_ = false;
    }
}

void MasterSlaveImpl::LogStatus() {
    LOG(INFO, "%s sync_idx_ = %d current_idx_ = %d applied_idx_ = %d callbacks_size = %d term_ = %ld",
        kLogPrefix.c_str(), sync_idx_, current_idx_, applied_idx_, callbacks_.size(), term_);
    StatusCode ret_a = logdb_->WriteMarker("applied_idx", applied_idx_);
    StatusCode ret_s = logdb_->WriteMarker("sync_idx", sync_idx_);
    if ((ret_a != kOK) || (ret_s != kOK)) {
        LOG(WARNING, "%s WriteMarker failed applied_idx_ = %ld sync_idx_ = %ld ",
            kLogPrefix.c_str(), applied_idx_, sync_idx_);
    }
    thread_pool_->DelayTask(5000, std::bind(&MasterSlaveImpl::LogStatus, this));
}

void MasterSlaveImpl::LogCleanUp() {
    MutexLock lock(&mu_);
    int64_t gc_index = std::max(sync_idx_ - 1, current_idx_ - FLAGS_master_slave_log_limit);
    StatusCode s = logdb_->DeleteUpTo(gc_index);
    if (s == kOK) {
        gc_idx_ = gc_index;
        LOG(INFO, "%s logdb gc upto %ld", kLogPrefix.c_str(), gc_idx_);
    } else {
        LOG(INFO, "%s logdb gc failed %ld reason %s", kLogPrefix.c_str(),
            gc_index, StatusCode_Name(s).c_str());
    }
    thread_pool_->DelayTask(FLAGS_master_log_gc_interval * 1000,
                            std::bind(&MasterSlaveImpl::LogCleanUp, this));
}

std::string MasterSlaveImpl::GetStatus() {
    if (is_leader_) {
        if (master_only_) {
            return "<font color=\"red\">MasterOnly</font>";
        } else {
            return "Master <a href=" + slave_addr_ + "/dfs>Slave</a>";
        }
    } else {
        return "Slave <a href=" + master_addr_ + "/dfs>Master</a>";
    }
}

void MasterSlaveImpl::CleanupLogdb() {
    delete logdb_;
    StatusCode s = LogDB::DestroyDB("./logdb");
    if (s != kOK) {
        LOG(FATAL, "%s DestroyDB failed", kLogPrefix.c_str());
    }
    DBOption option;
    option.log_size = FLAGS_logdb_log_size;
    LogDB::Open("./logdb", option, &logdb_);
    if (logdb_ == NULL) {
        LOG(FATAL, "%s init logdb failed", kLogPrefix.c_str());
    }
    ++term_;
    s = logdb_->WriteMarker("term", term_);
    if (s != kOK) {
        LOG(FATAL, "%s Write marker term %ld failed", kLogPrefix.c_str(), term_);
    }
    LOG(INFO, "%s Cleanup logdb done, term_ = %ld", kLogPrefix.c_str(), term_);
}

} // namespace bfs
} // namespace baidu
