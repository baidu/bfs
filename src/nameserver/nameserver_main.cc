// Copyright (c) 2014, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#include <stdio.h>

#include <sofa/pbrpc/pbrpc.h>
#include <gflags/gflags.h>

#include "common/logging.h"
#include "nameserver/nameserver_impl.h"

DECLARE_string(flagfile);
DECLARE_string(nameserver_port);
DECLARE_int32(nameserver_log_level);

int main(int argc, char* argv[])
{
    FLAGS_flagfile = "./bfs.flag";
    ::google::ParseCommandLineFlags(&argc, &argv, false);
    common::SetLogLevel(FLAGS_nameserver_log_level);



    // Service
    bfs::NameServerImpl* nameserver_service = new bfs::NameServerImpl();

    // rpc_server
    sofa::pbrpc::RpcServerOptions options;
        
    sofa::pbrpc::RpcServer rpc_server(options);

    // Register
    if (!rpc_server.RegisterService(nameserver_service)) {
            return EXIT_FAILURE;
    }
    sofa::pbrpc::Servlet webservice =
        sofa::pbrpc::NewPermanentExtClosure(nameserver_service, &bfs::NameServerImpl::WebService);
    rpc_server.RegisterWebServlet("/dfs", webservice);

    // Start
    std::string server_host = std::string("0.0.0.0:") + FLAGS_nameserver_port;
    if (!rpc_server.Start(server_host)) {
            return EXIT_FAILURE;
    }

    rpc_server.Run();

    delete webservice;
    return EXIT_SUCCESS;
}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
