// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//

#ifndef  BFS_NAMESERVER_MASTER_SLAVE_H_
#define  BFS_NAMESERVER_MASTER_SLAVE_H_

#include <string>
#include <map>
#include <boost/function.hpp>
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
    virtual void Init(boost::function<void (const std::string& log, int64_t)> callback,
                      boost::function<void (int64_t, std::string*, bool*)> scan_func);
    virtual bool IsLeader(std::string* leader_addr = NULL);
    virtual bool Log(const std::string& entry, int timeout_ms = 10000);
    virtual void Log(const std::string& entry, boost::function<void (int64_t)> callback);
    virtual void SwitchToLeader();
    std::string GetStatus();

    // rpc
    void AppendLog(::google::protobuf::RpcController* controller,
                   const master_slave::AppendLogRequest* request,
                   master_slave::AppendLogResponse* response,
                   ::google::protobuf::Closure* done);
    void WriteSnapshot(::google::protobuf::RpcController* controller,
                       const master_slave::WriteSnapshotReqeust* request,
                       master_slave::WriteSnapshotResponse* response,
                       ::google::protobuf::Closure* done);
private:
    void BackgroundLog();
    void ReplicateLog();
    bool ReplicateSnapshot();
    void ClearLog();
    void LogStatus();
    void ProcessCallbck(int64_t index, bool timeout_check);

private:
    RpcClient* rpc_client_;
    master_slave::MasterSlave_Stub* slave_stub_;

    boost::function<void (const std::string& log, int64_t)> log_callback_;
    boost::function<void (int64_t, std::string*, bool*)> scan_func_;
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
    int64_t snapshot_idx_;  // snpashot's index
    int64_t snapshot_step_; // snpahsot's step, index:step pin points a batch of log entry

    std::map<int64_t, boost::function<void (int64_t)> > callbacks_;
};

} // namespace bfs
} // namespace baidu

#endif  //BFS_NAMESERVER_MASTER_SLAVE_H_
