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

namespace baidu {
namespace bfs {

MasterSlaveImpl::MasterSlaveImpl() : exiting_(false), master_only_(false),
                                     cond_(&mu_), log_done_(&mu_), current_idx_(-1),
                                     applied_idx_(-1), sync_idx_(-1) {
    std::vector<std::string> nodes;
    common::SplitString(FLAGS_nameserver_nodes, ",", &nodes);
    std::string this_server = nodes[FLAGS_node_index];
    std::string another_server;
    if (FLAGS_node_index == 0) {
        another_server = nodes[1];
    } else if (FLAGS_node_index == 1) {
        another_server = nodes[0];
    } else {
        LOG(FATAL, "[Sync] Nameserver does not belong to this cluster");
    }
    master_addr_ = FLAGS_master_slave_role == "master" ? this_server: another_server;
    slave_addr_ = FLAGS_master_slave_role == "slave" ? this_server: another_server;
    is_leader_ = FLAGS_master_slave_role == "master";
    if (IsLeader()) {
        LOG(INFO, "[Sync] I am Leader");
    } else {
        LOG(INFO, "[Sync] I am Slave");
    }
    thread_pool_ = new common::ThreadPool(10);
    mkdir("./logdb", 0755);
    logdb_ = new LogDB("./logdb");
}

void MasterSlaveImpl::Init(boost::function<void (const std::string& log)> callback) {
    log_callback_ = callback;
    if (logdb_->GetLargestIdx(&current_idx_) == kReadError) {
        LOG(FATAL, "[Sync] Read current_idx_ failed");
    }
    if (logdb_->ReadMarker("applied_idx", &applied_idx_) == kReadError) {
        LOG(FATAL, "[Sync] ReadMarker applied_idx_ failed");
    }
    if (logdb_->ReadMarker("sync_idx", &sync_idx_) == kReadError) {
        LOG(FATAL, "[Sync] ReadMarker sync_idx_ failed");
    }
    LOG(INFO, "[Sync] set current_idx_ = %ld, applied_idx_ = %ld, sync_idx_ = %ld ",
            current_idx_, applied_idx_, sync_idx_);
    assert(applied_idx_ <= current_idx_ && sync_idx_ <= current_idx_);
    while (applied_idx_ < current_idx_) {
        std::string entry;
        bool ret = logdb_->Read(applied_idx_ + 1, &entry);
        if (!ret) {
            LOG(FATAL, "[Sync] read logdb failed index %ld ", applied_idx_ + 1);
        }
        if (!entry.empty()) {
            log_callback_(entry);
        }
        applied_idx_++;
    }

    rpc_client_ = new RpcClient();
    rpc_client_->GetStub(slave_addr_, &slave_stub_);
    if (IsLeader()) {
        worker_.Start(boost::bind(&MasterSlaveImpl::BackgroundLog, this));
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
        LOG(FATAL, "[Sync] write logdb failed index %ld", current_idx_ + 1);
    }
    current_idx_++;
    cond_.Signal();
    mu_.Unlock();
    // slave is way behind, do no wait
    if (master_only_ && sync_idx_ < current_idx_ - 1) {
        LOG(WARNING, "[Sync] Sync in maset-only mode, do not wait");
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
                LOG(INFO, "[Sync] leaves master-only mode");
                master_only_ = false;
            }
            LOG(INFO, "[Sync] sync log takes %ld ms", common::timer::get_micros() - start_point);
            return true;
        } else {
            break;
        }
    }
    // log replicate time out
    LOG(WARNING, "[Sync] Sync log timeout, Sync is in master-only mode");
    master_only_ = true;
    return true;
}

void MasterSlaveImpl::Log(const std::string& entry, boost::function<void (bool)> callback) {
    if (!IsLeader()) {
        return;
    }
    MutexLock lock(&mu_);
    if (logdb_->Write(current_idx_ + 1, entry) != kOK) {
        LOG(FATAL, "[Sync] write logdb failed index %ld ", current_idx_ + 1);
    }
    current_idx_++;
    if (master_only_ && sync_idx_ < current_idx_ - 1) { // slave is behind, do not wait
        callbacks_.insert(std::make_pair(current_idx_, callback));
        thread_pool_->AddTask(boost::bind(&MasterSlaveImpl::PorcessCallbck,this,
                                            current_idx_, true));
    } else {
        callbacks_.insert(std::make_pair(current_idx_, callback));
        LOG(DEBUG, "[Sync] insert callback index = %d", current_idx_);
        thread_pool_->DelayTask(10000, boost::bind(&MasterSlaveImpl::PorcessCallbck,
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
    worker_.Start(boost::bind(&MasterSlaveImpl::BackgroundLog, this));
    is_leader_ = true;
    LOG(INFO, "[Sync] node switch to leader");
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
    // expect index to be current_idx_ + 1
    if (request->index() > current_idx_ + 1) {
        response->set_index(current_idx_ + 1);
        response->set_success(false);
        done->Run();
        return;
    } else if (request->index() <= current_idx_) {
        LOG(INFO, "[Sync] out-date log request %ld, current_idx_ %ld",
            request->index(), current_idx_);
        response->set_index(-1);
        response->set_success(false);
        done->Run();
        return;
    }
    mu_.Lock();
    if (logdb_->Write(current_idx_ + 1, request->log_data()) != kOK) {
        LOG(FATAL, "[Sync] Write logdb_ failed current_idx_ = %ld ", current_idx_ + 1);
    }
    current_idx_++;
    mu_.Unlock();
    log_callback_(request->log_data());
    applied_idx_ = current_idx_;
    response->set_success(true);
    done->Run();
}

void MasterSlaveImpl::BackgroundLog() {
    while (true) {
        MutexLock lock(&mu_);
        while (!exiting_ && sync_idx_ == current_idx_) {
            LOG(DEBUG, "[Sync] BackgroundLog waiting...");
            cond_.Wait();
        }
        if (exiting_) {
            return;
        }
        LOG(DEBUG, "[Sync] BackgroundLog logging...");
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
        LOG(DEBUG, "[Sync] ReplicateLog sync_idx_ = %d, current_idx_ = %d", sync_idx_, current_idx_);
        mu_.Unlock();
        std::string entry;
        if (logdb_->Read(sync_idx_ + 1, &entry) != kOK) {
            LOG(FATAL, "[Sync] Read logdb_ failed sync_idx_ = %ld ", sync_idx_ + 1);
        }
        master_slave::AppendLogRequest request;
        master_slave::AppendLogResponse response;
        request.set_log_data(entry);
        request.set_index(sync_idx_ + 1);
        while (!rpc_client_->SendRequest(slave_stub_, &master_slave::MasterSlave_Stub::AppendLog,
                &request, &response, 15, 1)) {
            LOG(WARNING, "[Sync] Replicate log failed index = %d, current_idx_ = %d",
                sync_idx_ + 1, current_idx_);
            sleep(5);
        }
        if (!response.success()) { // log mismatch
            MutexLock lock(&mu_);
            if (response.index() != -1) {
                sync_idx_ = response.index() - 1;
                LOG(INFO, "[Sync] set sync_idx_ to %d", sync_idx_);
            }
            continue;
        }
        thread_pool_->AddTask(boost::bind(&MasterSlaveImpl::PorcessCallbck, this, sync_idx_ + 1, false));
        mu_.Lock();
        sync_idx_++;
        LOG(DEBUG, "[Sync] Replicate log done. sync_idx_ = %d, current_idx_ = %d",
                sync_idx_ , current_idx_);
        mu_.Unlock();
    }
    applied_idx_ = current_idx_;
    log_done_.Signal();
}

void MasterSlaveImpl::PorcessCallbck(int64_t index, bool timeout_check) {
    boost::function<void (bool)> callback;
    MutexLock lock(&mu_);
    std::map<int64_t, boost::function<void (bool)> >::iterator it = callbacks_.find(index);
    if (it != callbacks_.end()) {
        callback = it->second;
        callbacks_.erase(it);
        LOG(DEBUG, "[Sync] calling callback %d", it->first);
        mu_.Unlock();
        callback(true);
        mu_.Lock();
        if (index > applied_idx_) {
            applied_idx_ = index;
        }
        if (timeout_check && !master_only_) {
            LOG(WARNING, "[Sync] ReplicateLog sync_idx_ = %d timeout, enter master-only mode",
                index);
            master_only_ = true;
            return;
        }
    }
    if (master_only_ && index == current_idx_) {
        LOG(INFO, "[Sync] leaves master-only mode");
        master_only_ = false;
    }
}

void MasterSlaveImpl::LogStatus() {
    LOG(INFO, "[Sync] sync_idx_ = %d, current_idx_ = %d, applied_idx_ = %d, callbacks_ size = %d",
        sync_idx_, current_idx_, applied_idx_, callbacks_.size());
    bool ret_a = logdb_->WriteMarker("applied_idx", applied_idx_);
    bool ret_s = logdb_->WriteMarker("sync_idx", sync_idx_);
    if (!ret_a || ret_s) {
        LOG(WARNING, "[Sync] WriteMarker failed applied_idx_ = %ld sync_idx_ = %ld ",
                applied_idx_, sync_idx_);
    }
    thread_pool_->DelayTask(5000, boost::bind(&MasterSlaveImpl::LogStatus, this));
}

} // namespace bfs
} // namespace baidu
