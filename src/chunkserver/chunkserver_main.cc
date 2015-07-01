// Copyright (c) 2014, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#include <stdio.h>
#include <signal.h>

#include <sofa/pbrpc/pbrpc.h>
#include <gflags/gflags.h>

#include "chunkserver/chunkserver_impl.h"
#include "common/logging.h"

DECLARE_string(flagfile);
DECLARE_string(chunkserver_port);
DECLARE_string(block_store_path);
DECLARE_string(chunkserver_warninglog);
DECLARE_int32(chunkserver_log_level);

static volatile bool s_quit = false;
static void SignalIntHandler(int /*sig*/)
{
    s_quit = true;
}

int main(int argc, char* argv[])
{
    FLAGS_flagfile = "./bfs.flag";
    ::google::ParseCommandLineFlags(&argc, &argv, false);
    common::SetLogLevel(FLAGS_chunkserver_log_level);
    common::SetWarningFile(FLAGS_chunkserver_warninglog.c_str());

    sofa::pbrpc::RpcServerOptions options;
    options.work_thread_num = 8;
    sofa::pbrpc::RpcServer rpc_server(options);

    bfs::ChunkServerImpl* chunkserver_service = new bfs::ChunkServerImpl();

    if (!rpc_server.RegisterService(chunkserver_service)) {
            return EXIT_FAILURE;
    }

    std::string server_host = std::string("0.0.0.0:") + FLAGS_chunkserver_port;
    if (!rpc_server.Start(server_host)) {
            return EXIT_FAILURE;
    }

    signal(SIGINT, SignalIntHandler);
    signal(SIGTERM, SignalIntHandler);
    while (!s_quit) {
        sleep(1);
    }   

    return EXIT_SUCCESS;
}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
