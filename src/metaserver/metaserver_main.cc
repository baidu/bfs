// Copyright (c) 2014, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//

#include <stdio.h>

#include <sofa/pbrpc/pbrpc.h>
#include <gflags/gflags.h>
#include <common/logging.h>
#include <common/string_util.h>

#include "metaserver/metaserver_impl.h"
#include "version.h"

DECLARE_string(flagfile);
DECLARE_string(metaserver_nodes);
DECLARE_int32(node_index);
DECLARE_int32(metaserver_log_level);
DECLARE_string(metaserver_logfile);
DECLARE_string(metaserver_warninglog);
DECLARE_string(bfs_log);
DECLARE_int32(bfs_log_size);
DECLARE_int32(bfs_log_limit);

int main(int argc, char* argv[])
{
    if (argc > 1) {
        std::string ext_cmd = argv[1];
        if (ext_cmd == "version") {
            PrintSystemVersion();
            return 0;
        }
    }
    if (FLAGS_flagfile == "") {
        FLAGS_flagfile = "./bfs.flag";
    }
    ::google::ParseCommandLineFlags(&argc, &argv, false);
    if (FLAGS_bfs_log != "") {
        baidu::common::SetLogFile(FLAGS_bfs_log.c_str());
        baidu::common::SetLogSize(FLAGS_bfs_log_size);
        baidu::common::SetLogSizeLimit(FLAGS_bfs_log_limit);
    }
    ::baidu::common::SetLogLevel(FLAGS_metaserver_log_level);
    ::baidu::common::SetWarningFile(FLAGS_metaserver_warninglog.c_str());

    LOG(baidu::common::INFO, "MetaServer start ...");

    // rpc_server
    sofa::pbrpc::RpcServerOptions options;

    sofa::pbrpc::RpcServer rpc_server(options);

    // Server
    baidu::bfs::MetaServerImpl* metaserver_service = new baidu::bfs::MetaServerImpl(sync);

    // Register
    if (!rpc_server.RegisterService(metaserver_service)) {
        return EXIT_FAILURE;
    }

    // Start
    std::vector<std::string> metaserver_nodes;
    baidu::common::SplitString(FLAGS_metaserver_nodes, ",", &metaserver_nodes);
    if (static_cast<int>(metaserver_nodes.size()) <= FLAGS_node_index) {
        LOG(baidu::common::FATAL, "Bad nodes or index: %s, %d",
            FLAGS_metaserver_nodes.c_str(), FLAGS_node_index);
        return EXIT_FAILURE;
    }
    std::string server_addr = metaserver_nodes[FLAGS_node_index];
    std::string listen_addr = std::string("0.0.0.0") + server_addr.substr(server_addr.rfind(':'));
    if (!rpc_server.Start(listen_addr)) {
        return EXIT_FAILURE;
    }
    LOG(baidu::common::INFO, "RpcServer start.");

    rpc_server.Run();

    //delete webservice;
    LOG(baidu::common::WARNING, "Nameserver exit");
    return EXIT_SUCCESS;
}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
