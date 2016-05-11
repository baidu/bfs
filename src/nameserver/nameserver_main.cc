// Copyright (c) 2014, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#include <stdio.h>

#include <sofa/pbrpc/pbrpc.h>
#include <gflags/gflags.h>
#include <common/logging.h>

#include "nameserver/nameserver_impl.h"
#include "nameserver/raft_impl.h"
#include "nameserver/sync.h"
#include "version.h"

DECLARE_string(flagfile);
DECLARE_string(nameserver_port);
DECLARE_int32(nameserver_log_level);
DECLARE_string(nameserver_logfile);
DECLARE_string(nameserver_warninglog);
DECLARE_string(bfs_log);
DECLARE_int32(bfs_log_size);
DECLARE_string(ha_strategy);

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
    }
    ::baidu::common::SetLogLevel(FLAGS_nameserver_log_level);
    ::baidu::common::SetWarningFile(FLAGS_nameserver_warninglog.c_str());

    LOG(baidu::common::INFO, "NameServer start ...");

    // rpc_server
    sofa::pbrpc::RpcServerOptions options;

    sofa::pbrpc::RpcServer rpc_server(options);

    // Server
    baidu::bfs::Sync* sync = NULL;
    google::protobuf::Service* sync_service = NULL;
    if (FLAGS_ha_strategy == "master_slave") {
        baidu::bfs::MasterSlaveImpl* base = new baidu::bfs::MasterSlaveImpl();
        sync = base;
        sync_service = base;
        LOG(baidu::common::INFO, "master_slave");
        const google::protobuf::ServiceDescriptor* bug = sync_service->GetDescriptor();
        LOG(baidu::common::INFO, "bugname=%s", bug->name().c_str());
    } else if (FLAGS_ha_strategy == "raft") {
        // TODO: not working yet...
        baidu::bfs::RaftImpl* raft_impl = new baidu::bfs::RaftImpl();
        sync = raft_impl;
        sync_service = raft_impl->GetService();
    } else {
        LOG(baidu::common::FATAL, "NameServer start with unknow ha strategy");
    }

    baidu::bfs::NameServerImpl* nameserver_service = new baidu::bfs::NameServerImpl(sync);

    // Register
    if (!rpc_server.RegisterService(nameserver_service)) {
        return EXIT_FAILURE;
    }
    if (!rpc_server.RegisterService(sync_service)) {
        return EXIT_FAILURE;
    }

    sofa::pbrpc::Servlet webservice =
        sofa::pbrpc::NewPermanentExtClosure(nameserver_service, &baidu::bfs::NameServerImpl::WebService);
    rpc_server.RegisterWebServlet("/dfs", webservice);

    // Start
    std::string server_host = std::string("0.0.0.0:") + FLAGS_nameserver_port;
    if (!rpc_server.Start(server_host)) {
            return EXIT_FAILURE;
    }

    rpc_server.Run();

    //delete webservice;
    LOG(baidu::common::WARNING, "Nameserver exit");
    return EXIT_SUCCESS;
}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
