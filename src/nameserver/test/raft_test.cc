// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#include <stdio.h>
#include <boost/bind.hpp>

#include <sofa/pbrpc/pbrpc.h>
#include <gflags/gflags.h>
#include <common/logging.h>

#include "nameserver/raft_node.h"

DECLARE_string(flagfile);
DECLARE_string(nameserver_port);
DECLARE_int32(nameserver_log_level);
DECLARE_string(nameserver_logfile);
DECLARE_string(nameserver_warninglog);
DECLARE_string(bfs_log);
DECLARE_int32(bfs_log_size);

namespace baidu {
namespace bfs {

class RaftTest {
public:
    RaftTest(RaftNodeImpl* raft_node) : raft_node_(raft_node), applied_index_(0) {
    }
    void LogCallback(const std::string& log) {
        int t = 0;
        for (uint32_t i = 9; i < log.size(); i++) {
            t *= 10;
            t += log[i] - '0';
        }
        LOG(INFO, "[LogCallback] %d %s", t, log.c_str());
        applied_index_ = t;
    }
    void Run() {
        std::string leader;
        std::string self("127.0.0.1:");
        self += FLAGS_nameserver_port;
        raft_node_->RegisterCallback(boost::bind(&RaftTest::LogCallback, this, _1));
        while (applied_index_ < 10000) {
            if (raft_node_->GetLeader(&leader) && leader == self) {
                applied_index_ ++;
                char buf[64];
                snprintf(buf, sizeof(buf), "LogEntry-%05d", applied_index_);
                bool ret = raft_node_->AppendLog(buf);
                LOG(INFO, "[RaftTest] Append log: \"%s\" return %d", buf, ret);
            } else {
                LOG(INFO, "[RaftTest] Leader is %s", leader.c_str());
            }
            sleep(1);
        }
    }
private:
    RaftNodeImpl* raft_node_;
    int64_t applied_index_;
};

}
}

int main(int argc, char* argv[])
{
    FLAGS_flagfile = "./bfs.flag";
    ::google::ParseCommandLineFlags(&argc, &argv, false);
    if (FLAGS_bfs_log != "") {
        baidu::common::SetLogFile(FLAGS_bfs_log.c_str());
        baidu::common::SetLogSize(FLAGS_bfs_log_size);
    }
    ::baidu::common::SetLogLevel(FLAGS_nameserver_log_level);
    ::baidu::common::SetWarningFile(FLAGS_nameserver_warninglog.c_str());

    LOG(baidu::common::INFO, "NameServer start ...");

    // Service
    baidu::bfs::RaftNodeImpl* raft_service = new baidu::bfs::RaftNodeImpl();

    // rpc_server
    sofa::pbrpc::RpcServerOptions options;

    sofa::pbrpc::RpcServer rpc_server(options);

    // Register
    if (!rpc_server.RegisterService(raft_service)) {
        return EXIT_FAILURE;
    }

    // Start
    std::string server_host = std::string("0.0.0.0:") + FLAGS_nameserver_port;
    if (!rpc_server.Start(server_host)) {
        return EXIT_FAILURE;
    }

    baidu::bfs::RaftTest test(raft_service);
    test.Run();

    //delete webservice;
    LOG(baidu::common::WARNING, "Nameserver exit");
    return EXIT_SUCCESS;
}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
