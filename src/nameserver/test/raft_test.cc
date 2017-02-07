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
#include "proto/raft_kv.pb.h"

DECLARE_string(flagfile);
DECLARE_string(nameserver_nodes);
DECLARE_int32(node_index);
DECLARE_int32(nameserver_election_timeout);
DECLARE_int32(nameserver_log_level);
DECLARE_string(nameserver_logfile);
DECLARE_string(nameserver_warninglog);
DECLARE_string(bfs_log);
DECLARE_int32(bfs_log_size);
DECLARE_string(raftdb_path);

namespace baidu {
namespace bfs {
namespace raft {
class KvServer : public RaftKv {
public:
    KvServer(RaftNodeImpl* raft_node) : raft_node_(raft_node), applied_index_(0) {
        raft_node_->Init(std::bind(&KvServer::LogCallback, this, std::placeholders::_1),
                         std::function<void (int32_t, std::string*)>());
    }
    void LogCallback(const std::string& log) {
        PutRequest request;
        if (!request.ParseFromString(log)) {
            LOG(FATAL, "Parse log fail");
            return;
        }
        if (request.del()) {
            db_.erase(request.key());
        } else {
            db_[request.key()] = request.value();
        }
        LOG(INFO, "%s %s -> %s", request.del()?"Delete":"Put",
            request.key().c_str(), request.value().c_str());
    }
    virtual void Put(::google::protobuf::RpcController* controller,
                     const PutRequest* request,
                     PutResponse* response,
                     ::google::protobuf::Closure* done) {
        if (!raft_node_->GetLeader(NULL)) {
            response->set_status(1);
            done->Run();
            return;
        }
        std::string log;
        request->SerializeToString(&log);
        LOG(INFO, "Put start: %s -> %s", request->key().c_str(), request->value().c_str());
        raft_node_->AppendLog(log,
            std::bind(&KvServer::PutCallback, this, request, response, done, std::placeholders::_1));
    }
    void PutCallback(const PutRequest* request,
                     PutResponse* response,
                     ::google::protobuf::Closure* done,
                     bool ret) {
        if (ret != 0) {
            response->set_status(2);
        }
        response->set_status(0);
        LOG(INFO, "Put done: %s %s -> %s",
            ret ? "sucess" : "fail",
            request->key().c_str(), request->value().c_str());
        done->Run();
    }
    virtual void Get(::google::protobuf::RpcController* controller,
                     const GetRequest* request,
                     GetResponse* response,
                     ::google::protobuf::Closure* done) {
        const std::string& key = request->key();
        std::map<std::string, std::string>::const_iterator it = db_.find(key);
        int ret = 0;
        if (it == db_.end()) {
            ret = 404;
        } else {
            response->set_value(it->second);
        }
        LOG(INFO, "Get %s %d return %s",
            key.c_str(), ret, response->value().c_str());
        response->set_status(ret);
        done->Run();
    }
private:
    RaftNodeImpl* raft_node_;
    std::map<std::string, std::string> db_;
    int64_t applied_index_;
};

}
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

    LOG(baidu::common::INFO, "RaftKv start ...");

    // Service
    baidu::bfs::RaftNodeImpl* raft_service =
        new baidu::bfs::RaftNodeImpl(FLAGS_nameserver_nodes, FLAGS_node_index,
                                     FLAGS_nameserver_election_timeout, FLAGS_raftdb_path);
    baidu::bfs::raft::KvServer* kv_service =
        new baidu::bfs::raft::KvServer(raft_service);
    // rpc_server
    sofa::pbrpc::RpcServerOptions options;
    sofa::pbrpc::RpcServer rpc_server(options);

    // Register
    if (!rpc_server.RegisterService(raft_service)) {
        return EXIT_FAILURE;
    }
    if (!rpc_server.RegisterService(kv_service)) {
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

    LOG(baidu::common::INFO, "RpcServer start.");
    rpc_server.Run();

    //delete webservice;
    LOG(baidu::common::WARNING, "RaftKv exit");
    return EXIT_SUCCESS;
}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
