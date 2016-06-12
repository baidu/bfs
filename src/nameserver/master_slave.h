// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//

#ifndef  BFS_NAMESERVER_MASTER_SLAVE_H_
#define  BFS_NAMESERVER_MASTER_SLAVE_H_

#include <string>
#include <stdio.h>
#include <map>
#include <boost/function.hpp>
#include <common/mutex.h>
#include <common/thread.h>
#include <common/thread_pool.h>

#include "nameserver/sync.h"
#include "proto/status_code.pb.h"
#include "proto/master_slave.pb.h"

namespace baidu {
namespace bfs {

class RpcClient;

class MasterSlaveImpl : public Sync, public master_slave::MasterSlave {
public:
    MasterSlaveImpl();
    virtual ~MasterSlaveImpl() {};
    virtual void Init(boost::function<void (const std::string& log)> callback);
    virtual bool IsLeader(std::string* leader_addr = NULL);
    virtual bool Log(const std::string& entry, int timeout_ms = 10000);
    virtual void Log(const std::string& entry, boost::function<void (bool)> callback);
    virtual void SwitchToLeader();

    // rpc
    void AppendLog(::google::protobuf::RpcController* controller,
                   const master_slave::AppendLogRequest* request,
                   master_slave::AppendLogResponse* response,
                   ::google::protobuf::Closure* done);
    int LogLocal(const std::string& entry);

private:
    bool ReadEntry(std::string* entry);
    void BackgroundLog();
    void ReplicateLog();
    void LogStatus();
    void PorcessCallbck(int offset, int len, bool timeout_check);

private:
    RpcClient* rpc_client_;
    master_slave::MasterSlave_Stub* slave_stub_;

    boost::function<void (const std::string& log)> log_callback_;
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

    int log_;
    FILE* read_log_;
    int scan_log_;
    int current_offset_;
    int applied_offset_;
    int sync_offset_;

    std::map<int, boost::function<void (bool)> > callbacks_;
};

} // namespace bfs
} // namespace baidu

#endif  //BFS_NAMESERVER_MASTER_SLAVE_H_
