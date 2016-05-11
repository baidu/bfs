// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//

#ifndef  BFS_NAMESERVER_SYNC_SYNC_H_
#define  BFS_NAMESERVER_SYNC_SYNC_H_

#include <string>
#include <boost/function.hpp>
#include <common/mutex.h>

#include "proto/status_code.pb.h"
#include "proto/master_slave.pb.h"

namespace baidu {
namespace bfs {

class RpcClient;

class Sync {
public:
    virtual ~Sync() {}
    virtual void Init() = 0;
    virtual bool IsLeader(std::string* leader_addr = NULL) = 0;
    virtual bool Log(const std::string& entry, int timeout_ms = 10000) = 0;
    virtual void Log(const std::string& entry, boost::function<void ()> callback) = 0;
    virtual void RegisterCallback(boost::function<void (const std::string& log)> callback) = 0;
    virtual int ScanLog() = 0;
    virtual int Next(char* entry) = 0;
};

class MasterSlaveImpl : public Sync, public master_slave::MasterSlave {
public:
    MasterSlaveImpl();
    virtual ~MasterSlaveImpl() {};
    virtual void Init();
    virtual bool IsLeader(std::string* leader_addr = NULL);
    virtual bool Log(const std::string& entry, int timeout_ms = 10000);
    virtual void Log(const std::string& entry, boost::function<void ()> callback);
    virtual void RegisterCallback(boost::function<void (const std::string& log)> callback);
    virtual int ScanLog();
    virtual int Next(char* entry);

    // rpc
    void AppendLog(::google::protobuf::RpcController* controller,
                   const master_slave::AppendLogRequest* request,
                   master_slave::AppendLogResponse* response,
                   ::google::protobuf::Closure* done);

private:
    void BackgroundLog();

private:
    RpcClient* rpc_client_;
    boost::function<void (const std::string& log)> log_callback_;
    Mutex mu_;
    CondVar cond_;
    bool master_only_;
    int log_;
    int scan_log_;
    int current_offset_;
    int sync_offset_;
    master_slave::MasterSlave_Stub* slave_stub_;
};

} // namespace bfs
} // namespace baidu

#endif  //BFS_NAMESERVER_SYNC_SYNC_H_
