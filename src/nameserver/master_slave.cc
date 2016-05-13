// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//

#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <common/string_util.h>
#include <common/logging.h>
#include <gflags/gflags.h>

#include "nameserver/sync.h"
#include "rpc/rpc_client.h"

DECLARE_string(slave_node);
DECLARE_string(master_slave_role);

namespace baidu {
namespace bfs {

MasterSlaveImpl::MasterSlaveImpl() : exiting_(false), master_only_(false), cond_(&mu_),
                                     read_log_(-1), scan_log_(-1),
                                     current_offset_(0), sync_offset_(0) {
}

void MasterSlaveImpl::Init() {
    // recover sync_offset_
    int fp = open("prog.log", O_RDONLY);
    if (fp < 0 && errno != ENOENT) {
        LOG(FATAL, "[Sync] open prog.log failed reason: %s", strerror(errno));
    }
    if (fp >= 0) {
        char buf[4];
        uint32_t ret = read(fp, buf, 4);
        if (ret == 4) {
            memcpy(&sync_offset_, buf, 4);
            LOG(INFO, "[Sync] set sync_offset_ to %d", sync_offset_);
        }
        close(fp);
    }

    log_ = open("sync.log", O_WRONLY | O_CREAT, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP);
    if (log_ < 0) {
        LOG(FATAL, "[Sync] open sync log failed reason: %s", strerror(errno));
    }
    current_offset_ = lseek(log_, 0, SEEK_END);
    LOG(INFO, "[Sync] set current_offset_ to %d", current_offset_);
    assert(current_offset_ >= sync_offset_);

    read_log_ = open("sync.log", O_RDONLY);
    if (read_log_ < 0)  {
        LOG(FATAL, "[Sync] open sync log for read failed reason: %s", strerror(errno));
    }
    int offset = lseek(read_log_, sync_offset_, SEEK_SET);
    assert(offset == sync_offset_);

    rpc_client_ = new RpcClient();
    rpc_client_->GetStub(FLAGS_slave_node, &slave_stub_);
    if (IsLeader()) {
        worker_.Start(boost::bind(&MasterSlaveImpl::BackgroundLog, this));
        logger_.Start(boost::bind(&MasterSlaveImpl::LogProgress, this));
    }
}

bool MasterSlaveImpl::IsLeader(std::string* leader_addr) {
    return FLAGS_master_slave_role == "master";
}

bool MasterSlaveImpl::Log(const std::string& entry, int timeout_ms) {
    if (!IsLeader()) {
        LOG(FATAL, "[Sync] slave does not need to log");
    }
    int w = write(log_, entry.c_str(), entry.length());
    assert(w >= 0);
    int last_offset = current_offset_;
    mu_.Lock();
    current_offset_ += w;
    cond_.Signal();
    mu_.Unlock();
    usleep(500);

    // slave is way behind, do no wait
    if (master_only_ && sync_offset_ < last_offset) {
        LOG(WARNING, "[Sync] Sync in maset-only mode, do not wait");
        return true;
    }

    for (int i = 0; i < timeout_ms / 1000; i++) {
        if (sync_offset_ == current_offset_) {
            if (master_only_) {
                LOG(INFO, "[Sync] leaves master-only mode");
                master_only_ = false;
            }
            return true;
        } else {
            LOG(INFO, "[Sync] waiting for ReplicateLog... timer %d", i);
        }
        sleep(1);
    }
    LOG(WARNING, "[Sync] Sync is in master-only mode");
    master_only_ = true;
    return true;
}

void MasterSlaveImpl::Log(const std::string& entry, boost::function<void ()> callback) {
    return;
}

void MasterSlaveImpl::RegisterCallback(boost::function<void (const std::string& log)> callback) {
    log_callback_ = callback;
}

int MasterSlaveImpl::ScanLog() {
    scan_log_ = open("sync.log", O_RDONLY);
    if (scan_log_ < 0 && errno == ENOENT) {
        LOG(INFO, "[Sync] can't find sync log %d", scan_log_);
        return -1;
    }
    assert(scan_log_ != -1);
    return scan_log_;
}

int MasterSlaveImpl::Next(char* entry) {
    if (scan_log_ < 0) {
        return -1;
    }
    char buf[4];
    uint32_t ret = read(scan_log_, buf, 4);
    assert(ret >= 0);
    if (ret == 0) {
        close(scan_log_);
        scan_log_ = 0;
        rename("sync.log", "sync.bak");
        return 0;
    } else if (ret < 4) {
        LOG(WARNING, "incomplete record");
        return ret;
    }
    uint32_t len;
    memcpy(&len, buf, 4);
    LOG(INFO, "[Sync] read_len=%u", len);
    ret = read(scan_log_, entry, len);
    if (ret < len) {
        LOG(WARNING, "incomplete record");
        return len;
    }
    assert(ret == len);
    return len;
}

void MasterSlaveImpl::AppendLog(::google::protobuf::RpcController* controller,
                                const master_slave::AppendLogRequest* request,
                                master_slave::AppendLogResponse* response,
                                ::google::protobuf::Closure* done) {
    int len = request->log_data().size();
    LOG(INFO, "[Sync] receive log len=%d", len);
    char buf[4];
    memcpy(buf, &len, 4);
    int ret = write(log_, buf, 4);
    assert(ret == 4);
    ret = write(log_, request->log_data().c_str(), len);
    assert(ret == len);
    log_callback_(request->log_data());
    response->set_success(true);
    done->Run();
}

void MasterSlaveImpl::BackgroundLog() {
    while (true) {
        MutexLock lock(&mu_);
        while (!exiting_ && sync_offset_ == current_offset_) {
            LOG(INFO, "[Sync] BackgroundLog waiting...");
            cond_.Wait();
        }
        if (exiting_) {
            return;
        }
        LOG(INFO, "[Sync] BackgroundLog logging...");
        mu_.Unlock();
        ReplicateLog();
        mu_.Lock();
    }
}

void MasterSlaveImpl::ReplicateLog() {
    while (sync_offset_ < current_offset_) {
        LOG(INFO, "[Sync] ReplicateLog sync_offset_ = %d, current_offset_ = %d",
                sync_offset_, current_offset_);
        if (read_log_ < 0) {
            LOG(FATAL, "[Sync] read_log_ error");
        }
        char buf[4];
        uint32_t ret = read(read_log_, buf, 4);
        if (ret < 4) {
            LOG(WARNING, "[Sync] read failed read length = %u", ret);
            assert(0);
            return;
        }
        uint32_t len;
        memcpy(&len, buf, 4);
        LOG(INFO, "[Sync] record length = %u", len);
        char* entry = new char[len];
        ret = read(read_log_, entry, len);
        if (ret < len) {
            LOG(WARNING, "[Sync] incomplete record");
            return;
        }
        master_slave::AppendLogRequest request;
        master_slave::AppendLogResponse response;
        request.set_log_data(std::string(entry, len));
        while (!rpc_client_->SendRequest(slave_stub_, &master_slave::MasterSlave_Stub::AppendLog,
               &request, &response, 15, 1)) {
            LOG(WARNING, "[Sync] Replicate log failed sync_offset_ = %d, current_offset_ = %d",
                sync_offset_, current_offset_);
            sleep(5);
        }
        sync_offset_ += 4 + len;
        LOG(INFO, "[Sync] Replicate log done. sync_offset_ = %d, current_offset_ = %d",
                sync_offset_, current_offset_);
        delete[] entry;
    }
}

void MasterSlaveImpl::LogProgress() {
    while (!exiting_) {
        sleep(10);
        int fp = open("prog.tmp", O_WRONLY | O_CREAT, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP);
        if (fp < 0) {
            LOG(FATAL, "[Sync] open prog.tmp failed reason: %s", strerror(errno));
        }
        char buf[4];
        memcpy(buf, &sync_offset_, 4);
        int ret = write(fp, buf, 4);
        if (ret == 4) {
            rename("prog.tmp", "prog.log");
        }
        close(fp);
    }

}
} // namespace bfs
} // namespace baidu
