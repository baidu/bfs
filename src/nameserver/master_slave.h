// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//

#ifndef  BFS_NAMESERVER_MASTER_SLAVE_H_
#define  BFS_NAMESERVER_MASTER_SLAVE_H_

#include <string>
#include <map>
#include <functional>
#include <common/mutex.h>
#include <common/thread.h>
#include <common/thread_pool.h>

#include "nameserver/sync.h"
#include "nameserver/logdb.h"
#include "proto/master_slave.pb.h"

namespace baidu {
namespace bfs {

class RpcClient;

class MasterSlaveImpl : public Sync, public master_slave::MasterSlave {
public:
    MasterSlaveImpl();
    virtual ~MasterSlaveImpl() {};
    virtual void Init(std::function<void (const std::string& log)> callback);
    virtual bool IsLeader(std::string* leader_addr = NULL);
    virtual bool Log(const std::string& entry, int timeout_ms = 10000);
    virtual void Log(const std::string& entry, std::function<void (bool)> callback);
    virtual void SwitchToLeader();

    // rpc
    void AppendLog(::google::protobuf::RpcController* controller,
                   const master_slave::AppendLogRequest* request,
                   master_slave::AppendLogResponse* response,
                   ::google::protobuf::Closure* done);

    std::string GetStatus();
private:
    void BackgroundLog();
    void ReplicateLog();
    void LogStatus();
    void PorcessCallbck(int64_t index, bool timeout_check);

private:
    RpcClient* rpc_client_;
    master_slave::MasterSlave_Stub* slave_stub_;

    std::function<void (const std::string& log)> log_callback_;
    bool exiting_;
    bool master_only_;
    bool is_leader_;
    std::string master_addr_;
    std::string slave_addr_;

    Mutex mu_;
    CondVar cond_;
    CondVar log_done_;
    common::Thread worker_;
    ThreadPool* thread_pool_;

    LogDB* logdb_;
    int64_t current_idx_;   // last log's index
    int64_t applied_idx_;   // last applied entry index
    int64_t sync_idx_;      // last entry index which slave has received

    std::map<int64_t, std::function<void (bool)> > callbacks_;
};

} // namespace bfs
} // namespace baidu

#endif  //BFS_NAMESERVER_MASTER_SLAVE_H_
