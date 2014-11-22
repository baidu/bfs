// Copyright (c) 2014, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#include <sofa/pbrpc/pbrpc.h>

#include "nameserver/nameserver_impl.h"

extern std::string FLAGS_nameserver_port;

int main(int argc, char* argv[])
{
    for (int i = 1; i < argc; i++) {
        char s[16];
        if (sscanf(argv[i], "--port=%s", s) == 1) {
            FLAGS_nameserver_port = s;
        } else {
            fprintf(stderr, "Invalid flag '%s'\n", argv[i]);
            exit(1);
        }
    }
    // rpc_server
    sofa::pbrpc::RpcServerOptions options;
    sofa::pbrpc::RpcServer rpc_server(options);

    // Service
    bfs::NameServer* nameserver_service = new bfs::NameServerImpl();

    // Register
    if (!rpc_server.RegisterService(nameserver_service)) {
            return EXIT_FAILURE;
    }

    // Start
    std::string server_host = std::string("0.0.0.0:") + FLAGS_nameserver_port;
    if (!rpc_server.Start(server_host)) {
            return EXIT_FAILURE;
    }

    rpc_server.Run();

    return EXIT_SUCCESS;
}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
