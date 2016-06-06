// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//

#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <common/string_util.h>
#include <common/logging.h>
#include <common/timer.h>
#include <gflags/gflags.h>

#include "nameserver/master_slave.h"
#include "rpc/rpc_client.h"

DECLARE_string(nameserver_nodes);
DECLARE_int32(node_index);
DECLARE_string(master_slave_role);

namespace baidu {
namespace bfs {

MasterSlaveImpl::MasterSlaveImpl() : exiting_(false), master_only_(false),
                                     cond_(&mu_),
                                     log_done_(&mu_), read_log_(NULL), scan_log_(-1),
                                     current_offset_(0), applied_offset_(0), sync_offset_(0) {
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
}

void MasterSlaveImpl::Init(boost::function<void (const std::string& log)> callback) {
    log_callback_ = callback;
    log_ = open("sync.log", O_WRONLY | O_CREAT, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP);
    if (log_ < 0) {
        LOG(FATAL, "[Sync] open sync log failed reason: %s", strerror(errno));
    }
    current_offset_ = lseek(log_, 0, SEEK_END);
    LOG(INFO, "[Sync] set current_offset_ to %d", current_offset_);

    read_log_ = fopen("sync.log", "r");
    if (read_log_ == NULL)  {
        LOG(FATAL, "[Sync] open sync log for read failed reason: %s", strerror(errno));
    }
    // redo log
    int fp = open("applied.log", O_RDONLY);
    if (fp < 0 && errno != ENOENT) {
        LOG(FATAL, "[Sync] open applied.log failed, reason: %s", strerror(errno));
    }
    if (fp >= 0) {
        char buf[4];
        int ret = read(fp, buf, 4);
        if (ret == 4) {
            memcpy(&applied_offset_, buf, 4);
            assert(applied_offset_ <= current_offset_);
            ret = fseek(read_log_, applied_offset_, SEEK_SET);
            if (ret != 0) {
                LOG(FATAL, "offset = %d applied_offset_ = %d", ftell(read_log_), applied_offset_);
            }
        }
        close(fp);
    }
    while (applied_offset_ < current_offset_) {
        std::string entry;
        if (!ReadEntry(&entry)) {
            assert(0);
        }
        log_callback_(entry);
        applied_offset_ += entry.length() + 4;
    }
    assert(applied_offset_ == current_offset_);

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
    int len = LogLocal(entry);
    int last_offset = current_offset_;
    current_offset_ += len;
    cond_.Signal();
    mu_.Unlock();
    int sync_offset = ftell(read_log_);
    // slave is way behind, do no wait
    if (master_only_ && sync_offset < last_offset) {
        LOG(WARNING, "[Sync] Sync in maset-only mode, do not wait");
        applied_offset_ = current_offset_;
        return true;
    }

    int64_t start_point = common::timer::get_micros();
    int64_t stop_point = start_point + timeout_ms * 1000;
    while (sync_offset != current_offset_ && common::timer::get_micros() < stop_point) {
        int wait_time = (stop_point - common::timer::get_micros()) / 1000;
        MutexLock lock(&mu_);
        if (log_done_.TimeWait(wait_time)) {
            if (sync_offset != current_offset_) {
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
    int len = LogLocal(entry);
    int last_offset = current_offset_;
    current_offset_ += len;
    if (master_only_ && ftell(read_log_) < last_offset) { // slave is behind, do not wait
        callbacks_.insert(std::make_pair(last_offset, callback));
        thread_pool_->AddTask(boost::bind(&MasterSlaveImpl::PorcessCallbck,this,
                                            last_offset, entry.length() + 4, true));
    } else {
        callbacks_.insert(std::make_pair(last_offset, callback));
        LOG(DEBUG, "[Sync] insert callback last_offset = %d", last_offset);
        thread_pool_->DelayTask(10000, boost::bind(&MasterSlaveImpl::PorcessCallbck,
                                                   this, last_offset, entry.length() + 4,
                                                   true));
        cond_.Signal();
    }
    return;
}

void MasterSlaveImpl::SwitchToLeader() {
    if (IsLeader()) {
        return;
    }
    is_leader_ = true;
    fseek(read_log_, 0, SEEK_SET);
    std::string old_master_addr = master_addr_;
    master_addr_ = slave_addr_;
    slave_addr_ = old_master_addr;
    rpc_client_->GetStub(slave_addr_, &slave_stub_);
    worker_.Start(boost::bind(&MasterSlaveImpl::BackgroundLog, this));
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
    if (request->offset() > current_offset_) {
        response->set_offset(current_offset_);
        response->set_success(false);
        done->Run();
        return;
    } else if (request->offset() < current_offset_) {
        LOG(INFO, "[Sync] out-date log request %d, current_offset_ %d",
            request->offset(), current_offset_);
        response->set_offset(-1);
        response->set_success(false);
        done->Run();
        return;
    }
    mu_.Lock();
    int len = LogLocal(request->log_data());
    mu_.Unlock();
    log_callback_(request->log_data());
    current_offset_ += len;
    applied_offset_ = current_offset_;
    response->set_success(true);
    done->Run();
}

bool MasterSlaveImpl::ReadEntry(std::string* entry) {
    char buf[4];
    int len;
    //int ret = read(read_log_, buf, 4);
    int ret = fread(buf, 4, 1, read_log_);
    assert(ret == 1);
    memcpy(&len, buf, 4);
    LOG(DEBUG, "[Sync] record length = %u", len);
    char* tmp = new char[len];
    //ret = read(read_log_, tmp, len);
    ret = fread(tmp, len, 1, read_log_);
    if (ret == 1) {
        entry->assign(tmp, len);
        delete[] tmp;
        return true;
    }
    delete[] tmp;
    return false;
}

void MasterSlaveImpl::BackgroundLog() {
    while (true) {
        MutexLock lock(&mu_);
        while (!exiting_ && ftell(read_log_) == current_offset_) {
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
    int sync_offset = ftell(read_log_);
    while (sync_offset < current_offset_) {
        mu_.Lock();
        if (sync_offset == current_offset_) {
            mu_.Unlock();
            break;
        }
        LOG(DEBUG, "[Sync] ReplicateLog sync_offset = %d, current_offset_ = %d",
                sync_offset, current_offset_);
        mu_.Unlock();
        std::string entry;
        if (!ReadEntry(&entry)) {
            LOG(WARNING, "[Sync] incomplete record");
            return;
        }
        master_slave::AppendLogRequest request;
        master_slave::AppendLogResponse response;
        request.set_log_data(entry);
        request.set_offset(sync_offset);
        while (!rpc_client_->SendRequest(slave_stub_,
                &master_slave::MasterSlave_Stub::AppendLog,
                &request, &response, 15, 1)) {
            LOG(WARNING, "[Sync] Replicate log failed sync_offset = %d, current_offset_ = %d",
                sync_offset, current_offset_);
            sleep(5);
        }
        if (!response.success()) { // log mismatch
            MutexLock lock(&mu_);
            if (response.offset() != -1) {
                sync_offset = response.offset();
                int ret = fseek(read_log_, sync_offset, SEEK_SET);
                assert(ret == 0);
                LOG(DEBUG, "[Sync] set sync_offset to %d", sync_offset);
            }
            continue;
        }
        thread_pool_->AddTask(boost::bind(&MasterSlaveImpl::PorcessCallbck,
                                this, sync_offset, entry.length() + 4, false));
        sync_offset = ftell(read_log_);
        mu_.Lock();
        LOG(DEBUG, "[Sync] Replicate log done. sync_offset = %d, current_offset_ = %d",
                sync_offset, current_offset_);
        mu_.Unlock();
    }
    applied_offset_ = current_offset_;
    log_done_.Signal();
}

int MasterSlaveImpl::LogLocal(const std::string& entry) {
    mu_.AssertHeld();
    int len = entry.length();
    write(log_, &len, 4);
    int w = write(log_, entry.c_str(), entry.length());
    assert(w >= 0);
    return w + 4;
}

void MasterSlaveImpl::PorcessCallbck(int offset, int len, bool timeout_check) {
    boost::function<void (bool)> callback;
    MutexLock lock(&mu_);
    std::map<int, boost::function<void (bool)> >::iterator it = callbacks_.find(offset);
    if (it != callbacks_.end()) {
        callback = it->second;
        callbacks_.erase(it);
        LOG(DEBUG, "[Sync] calling callback %d", it->first);
        mu_.Unlock();
        callback(true);
        mu_.Lock();
        if (offset + len > applied_offset_) {
            applied_offset_ = offset + len;
        }
        if (timeout_check && !master_only_) {
            LOG(WARNING, "[Sync] ReplicateLog sync_offset = %d timeout, enter master-only mode",
                offset);
            master_only_ = true;
            return;
        }
    }
    int sync_offset = ftell(read_log_);
    if (master_only_ && sync_offset + len >= current_offset_) {
        LOG(INFO, "[Sync] leaves master-only mode");
        master_only_ = false;
    }
}

void MasterSlaveImpl::LogStatus() {
    LOG(INFO, "[Sync] sync_offset = %d, current_offset_ = %d, applied_offset_ = %d, callbacks_ size = %d",
        ftell(read_log_), current_offset_, applied_offset_, callbacks_.size());
    int fp = open("applied.tmp", O_WRONLY | O_CREAT, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP);
    write(fp, &applied_offset_, 4);
    close(fp);
    rename("applied.tmp", "applied.log");
    thread_pool_->DelayTask(5000, boost::bind(&MasterSlaveImpl::LogStatus, this));
}

} // namespace bfs
} // namespace baidu
