// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#include <stdio.h>
#include <functional>

#include <sofa/pbrpc/pbrpc.h>
#include <gflags/gflags.h>
#include <common/logging.h>
#include <common/string_util.h>

#include "nameserver/raft_node.h"


DEFINE_string(raft_nodes, "127.0.0.1:8828,127.0.0.1:8829", "Nameserver cluster addresses");

DECLARE_string(flagfile);
DECLARE_string(nameserver_nodes);
DECLARE_int32(node_index);
DECLARE_int32(nameserver_log_level);
DECLARE_string(nameserver_logfile);
DECLARE_string(nameserver_warninglog);
DECLARE_string(bfs_log);
DECLARE_int32(bfs_log_size);
DECLARE_string(raftdb_path);

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
        std::vector<std::string> nodes;
        common::SplitString(FLAGS_nameserver_nodes, ",", &nodes);
        if (static_cast<int64_t>(nodes.size()) < FLAGS_node_index) {
            LOG(WARNING, "Load nameserver nodes fail");
            return;
        }
        std::string self = nodes[FLAGS_node_index];
        raft_node_->Init(std::bind(&RaftTest::LogCallback, this, _1));
        while (applied_index_ < 10000) {
            if (raft_node_->GetLeader(&leader) && leader == self) {
                applied_index_ ++;
                char buf[64];
                snprintf(buf, sizeof(buf), "LogEntry-%05ld", applied_index_);
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
    baidu::bfs::RaftNodeImpl* raft_service =
        new baidu::bfs::RaftNodeImpl(FLAGS_raft_nodes, FLAGS_node_index, FLAGS_raftdb_path);

    // rpc_server
    sofa::pbrpc::RpcServerOptions options;

    sofa::pbrpc::RpcServer rpc_server(options);

    // Register
    if (!rpc_server.RegisterService(raft_service)) {
        return EXIT_FAILURE;
    }

    // Start
    std::vector<std::string> nameserver_nodes;
    baidu::common::SplitString(FLAGS_nameserver_nodes, ",", &nameserver_nodes);
    if (static_cast<int>(nameserver_nodes.size()) <= FLAGS_node_index) {
        LOG(baidu::common::FATAL, "Bad nodes or index: %s, %d",
            FLAGS_nameserver_nodes.c_str(), FLAGS_node_index);
        return EXIT_FAILURE;
    }
    std::string server_addr = nameserver_nodes[FLAGS_node_index];
    std::string listen_addr = std::string("0.0.0.0") + server_addr.substr(server_addr.rfind(':'));
    if (!rpc_server.Start(server_addr)) {
        return EXIT_FAILURE;
    }

    baidu::bfs::RaftTest test(raft_service);
    test.Run();

    //delete webservice;
    LOG(baidu::common::WARNING, "Nameserver exit");
    return EXIT_SUCCESS;
}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
