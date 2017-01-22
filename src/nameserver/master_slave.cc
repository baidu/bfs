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

DECLARE_string(nameserver_nodes);
DECLARE_int32(node_index);
DECLARE_string(master_slave_role);
DECLARE_int64(master_slave_log_limit);
DECLARE_int32(master_log_gc_interval);

namespace baidu {
namespace bfs {

const std::string kLogPrefix = "\033[32m[Sync]\033[0m";

MasterSlaveImpl::MasterSlaveImpl() : slave_stub_(NULL), exiting_(false), master_only_(false),
                                     cond_(&mu_), log_done_(&mu_), current_idx_(-1),
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
    LogDB::Open("./logdb", option, &logdb_);
    if (logdb_ == NULL) {
        LOG(FATAL, "%s init logdb failed", kLogPrefix.c_str());
    }
    thread_pool_->DelayTask(FLAGS_master_log_gc_interval * 1000,
                            std::bind(&MasterSlaveImpl::LogCleanUp, this));
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
    LOG(INFO, "%s set current_idx_ = %ld, applied_idx_ = %ld, sync_idx_ = %ld ",
        kLogPrefix.c_str(), current_idx_, applied_idx_, sync_idx_);
    assert(applied_idx_ <= current_idx_ && sync_idx_ <= current_idx_);
    while (applied_idx_ < current_idx_) {
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
    cond_.Signal();
    mu_.Unlock();
    // slave is way behind, do no wait
    if (master_only_ && sync_idx_ < current_idx_ - 1) {
        LOG(WARNING, "%s Sync in master-only mode, do not wait", kLogPrefix.c_str());
        applied_idx_ = current_idx_;
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
    callbacks_.insert(std::make_pair(current_idx_, callback));
    if (master_only_ && sync_idx_ < current_idx_ - 1) { // slave is behind, do not wait
        thread_pool_->AddTask(std::bind(&MasterSlaveImpl::PorcessCallbck,this,
                                        current_idx_, true));
    } else {
        LOG(DEBUG, "%s insert callback index = %d", kLogPrefix.c_str(), current_idx_);
        thread_pool_->DelayTask(10000, std::bind(&MasterSlaveImpl::PorcessCallbck,
                                                 this, current_idx_, true));
        cond_.Signal();
    }
    return;
}

void MasterSlaveImpl::SwitchToLeader() {
    if (IsLeader()) {
        return;
    }
    sync_idx_ = -1;
    std::string old_master_addr = master_addr_;
    master_addr_ = slave_addr_;
    slave_addr_ = old_master_addr;
    rpc_client_->GetStub(slave_addr_, &slave_stub_);
    worker_.Start(std::bind(&MasterSlaveImpl::BackgroundLog, this));
    is_leader_ = true;
    master_only_ = true;
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
    mu_.Lock();
    if (logdb_->Write(current_idx_ + 1, request->log_data()) != kOK) {
        LOG(FATAL, "%s Write logdb_ failed current_idx_ = %ld ",
            kLogPrefix.c_str(), current_idx_ + 1);
    }
    current_idx_++;
    mu_.Unlock();
    log_callback_(request->log_data());
    applied_idx_ = current_idx_;
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
    int64_t seq = request->seq();
    if (seq == 0) {
        LOG(INFO, "%s Start to clean up the old namespace...", kLogPrefix.c_str());
        erase_callback_();
        LOG(INFO, "%s Done clean up the old namespace...", kLogPrefix.c_str());
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
        applied_idx_ = request->index();
        response->set_success(true);
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
        mu_.Lock();
        if (sync_idx_ == current_idx_) {
            mu_.Unlock();
            break;
        }
        LOG(DEBUG, "%s ReplicateLog sync_idx_ = %d, current_idx_ = %d",
            kLogPrefix.c_str(), sync_idx_, current_idx_);
        mu_.Unlock();
        std::string entry;
        if (logdb_->Read(sync_idx_ + 1, &entry) != kOK) {
            LOG(FATAL, "%s Read logdb_ failed sync_idx_ = %ld ",
                kLogPrefix.c_str(), sync_idx_ + 1);
        }
        master_slave::AppendLogRequest request;
        master_slave::AppendLogResponse response;
        request.set_log_data(entry);
        request.set_index(sync_idx_ + 1);
        if (!rpc_client_->SendRequest(slave_stub_,
                                      &master_slave::MasterSlave_Stub::AppendLog,
                                      &request, &response, 15, 1)) {
            LOG(WARNING, "%s Replicate log failed index = %d, current_idx_ = %d",
                kLogPrefix.c_str(), sync_idx_ + 1, current_idx_);
            EmptyLog();
            continue;
        }
        if (!response.success()) { // log mismatch
            MutexLock lock(&mu_);
            sync_idx_ = response.index() - 1;
            LOG(INFO, "%s set sync_idx_ to %d", kLogPrefix.c_str(), sync_idx_);
            if (sync_idx_ <= gc_idx_) {
                if (!SendSnapshot()) {
                    LOG(INFO, "%s Send snapshot failed", kLogPrefix.c_str());
                }
            }
            continue;
        }
        thread_pool_->AddTask(std::bind(&MasterSlaveImpl::PorcessCallbck,
                                        this, sync_idx_ + 1, false));
        mu_.Lock();
        sync_idx_++;
        LOG(DEBUG, "%s Replicate log done. sync_idx_ = %d, current_idx_ = %d",
            kLogPrefix.c_str(), sync_idx_ , current_idx_);
        mu_.Unlock();
    }
    applied_idx_ = current_idx_;
    log_done_.Signal();
}

void MasterSlaveImpl::EmptyLog() {
    master_slave::AppendLogRequest request;
    master_slave::AppendLogResponse response;
    request.set_index(-1);
    while (!rpc_client_->SendRequest(slave_stub_,
                                     &master_slave::MasterSlave_Stub::AppendLog,
                                     &request, &response, 15, 1)) {
        sleep(5);
    }
}

bool MasterSlaveImpl::SendSnapshot() {
    int64_t current_index = current_idx_ - 1; // minus one to make sure slave does not get one entry short
    bool ret = false;
    int64_t seq = 0;
    LOG(INFO, "%s Start sending snapshot", kLogPrefix.c_str());
    while (true) {
        std::string logstr;
        snapshot_callback_(0, &logstr);
        master_slave::SnapshotRequest request;
        master_slave::SnapshotResponse response;
        request.set_data(logstr);
        request.set_seq(seq);
        request.set_index(current_index);
        if (!rpc_client_->SendRequest(slave_stub_,
                                      &master_slave::MasterSlave_Stub::Snapshot,
                                      &request, &response, 15, 1)) {
            LOG(WARNING, "%s Send snapshot failed seq = %ld",
                kLogPrefix.c_str(), seq);
            break;
        }
        if (!response.success()) {
            break;
        }
        if (logstr.empty()) {
            LOG(INFO, "%s Send snapshot done seq = %ld", kLogPrefix.c_str(), seq);
            sync_idx_ = current_index;
            ret = true;
            break;
        }
        ++seq;
    }
    snapshot_callback_(0, NULL);
    return ret;
}

void MasterSlaveImpl::PorcessCallbck(int64_t index, bool timeout_check) {
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
        if (index > applied_idx_) {
            applied_idx_ = index;
        }
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
    LOG(INFO, "%s sync_idx_ = %d, current_idx_ = %d, applied_idx_ = %d, callbacks_ size = %d",
        kLogPrefix.c_str(), sync_idx_, current_idx_, applied_idx_, callbacks_.size());
    StatusCode ret_a = logdb_->WriteMarker("applied_idx", applied_idx_);
    StatusCode ret_s = logdb_->WriteMarker("sync_idx", sync_idx_);
    if ((ret_a != kOK) || (ret_s != kOK)) {
        LOG(WARNING, "%s WriteMarker failed applied_idx_ = %ld sync_idx_ = %ld ",
            kLogPrefix.c_str(), applied_idx_, sync_idx_);
    }
    thread_pool_->DelayTask(5000, std::bind(&MasterSlaveImpl::LogStatus, this));
}

void MasterSlaveImpl::LogCleanUp() {
    int64_t gc_index = std::max(sync_idx_ - 1, current_idx_ - FLAGS_master_slave_log_limit);
    StatusCode s = logdb_->DeleteUpTo(gc_index);
    if (s == kOK) {
        gc_idx_ = gc_index;
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
            return "master_only";
        } else {
            return "master_slave";
        }
    } else {
        return "slave";
    }
}

} // namespace bfs
} // namespace baidu
