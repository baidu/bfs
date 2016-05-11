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

MasterSlaveImpl::MasterSlaveImpl() : cond_(&mu_), master_only_(false), scan_log_(0),
                                     current_offset_(0), sync_offset_(0) {
    rpc_client_ = new RpcClient();
}

void MasterSlaveImpl::Init() {
    log_ = open("sync.log", O_WRONLY | O_CREAT, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP);
    if (log_ < 0) {
        LOG(FATAL, "open sync log failed reason:%s", strerror(errno));
    }
    rpc_client_->GetStub(FLAGS_slave_node, &slave_stub_);
}

bool MasterSlaveImpl::IsLeader(std::string* leader_addr) {
    return FLAGS_master_slave_role == "master";
}

bool MasterSlaveImpl::Log(const std::string& entry, int timeout_ms) {
    if (!IsLeader()) {
        LOG(FATAL, "slave does not need to log");
    }
    int w = write(log_, entry.c_str(), entry.length());
    assert(w >= 0);
    current_offset_ += w;

    master_slave::AppendLogRequest request;
    master_slave::AppendLogResponse response;
    request.set_log_data(entry);
    if (!rpc_client_->SendRequest(slave_stub_, &master_slave::MasterSlave_Stub::AppendLog, &request, &response, timeout_ms / 1000, 1)) {
        return false;
    }
    return true;
}

void MasterSlaveImpl::RegisterCallback(boost::function<void (const std::string& log)> callback) {
    log_callback_ = callback;
}

int MasterSlaveImpl::ScanLog() {
    scan_log_ = open("sync.log", O_RDONLY);
    if (scan_log_ < 0 && errno == ENOENT) {
        LOG(INFO, "can't find sync log %d", scan_log_);
        return -1;
    }
    assert(scan_log_ != -1);
    return scan_log_;
}

int MasterSlaveImpl::Next(char* entry) {
    if (scan_log_ <= 0) {
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
    log_callback_(request->log_data().substr(4));
    response->set_success(true);
    done->Run();
}

} // namespace bfs
} // namespace baidu
