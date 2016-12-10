// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//

#include <common/logging.h>
#include <common/string_util.h>
#include "proto/raft_kv.pb.h"
#include "rpc/rpc_client.h"

namespace baidu {
namespace bfs {

class KvClient {
public:
    KvClient(std::string nodes) {
        common::SplitString(nodes, ",", &node_addrs_);
        if (node_addrs_.size() < 1U) {
            LOG(FATAL, "Wrong server_addrs: %s", nodes.c_str());
        }
        servers_.resize(node_addrs_.size());
        for (uint32_t i = 0; i < node_addrs_.size(); i++) {
            rpc_client_.GetStub(node_addrs_[i], &servers_[i]);
        }
    }
    template <class Stub, class Request, class Response, class Callback>
    bool CallRaftMethod(void(Stub::*func)(google::protobuf::RpcController*,
                                          const Request*, Response*, Callback*),
                        const Request* request, Response* response) {
        bool ret = false;
        for (uint32_t i = 0; i < servers_.size(); i++) {
            ret = rpc_client_.SendRequest(servers_[i], func,
                                          request, response, 15, 1);
            LOG(INFO, "Send to %s return %s %d",
                node_addrs_[i].c_str(), ret?"sucess":"fail", response->status());
            if (ret && response->status() != 1) {
                break;
            }
        }
        return ret;
    }
    bool Put(const std::string& key, const std::string& value) {
        raft::PutRequest request;
        raft::PutResponse response;
        request.set_key(key);
        request.set_value(value);
        bool ret = CallRaftMethod(&raft::RaftKv_Stub::Put, &request, &response);
        if (!ret || response.status() != 0) {
            LOG(WARNING, "Put failed ret= %d status= %d", ret, response.status());
            return false;
        }
        LOG(INFO, "Put %s->%s", key.c_str(), value.c_str());
        return true;
    }
    bool Delete(const std::string& key) {
        raft::PutRequest request;
        raft::PutResponse response;
        request.set_key(key);
        request.set_del(true);
        bool ret = CallRaftMethod(&raft::RaftKv_Stub::Put, &request, &response);
        if (!ret || response.status() != 0) {
            LOG(WARNING, "Delete failed ret= %d status= %d", ret, response.status());
            return false;
        }
        LOG(INFO, "Delete %s done", key.c_str());
        return true;
    }
    bool Get(const std::string& key, std::string* value) {
        raft::GetRequest request;
        raft::GetResponse response;
        request.set_key(key);
        bool ret = CallRaftMethod(&raft::RaftKv_Stub::Get, &request, &response);
        if (!ret || response.status() != 0) {
            LOG(WARNING, "Get failed ret= %d status= %d", ret, response.status());
            return false;
        }
        if (value) *value = response.value();
        LOG(INFO, "Get %s return %s", key.c_str(), response.value().c_str());
        return true;
    }
private:
    std::vector<std::string> node_addrs_;
    std::vector<raft::RaftKv_Stub*> servers_;
    baidu::bfs::RpcClient rpc_client_;
};
}
}
void PrintUsage() {
    fprintf(stderr, "Usage:\n./kv_client get/put/delete key [value]\n");
}

const std::string server_nodes = "127.0.0.1:8828,127.0.0.1:8829,127.0.0.1:8830";
int main(int argc, char* argv[]) {
    if (argc < 3) {
        PrintUsage();
        return 1;
    }
    baidu::bfs::KvClient kv_client(server_nodes);
    if (strcmp(argv[1], "put") == 0 ) {
        if (argc < 4) {
            PrintUsage();
            return 1;
        }
        if (!kv_client.Put(argv[2], argv[3])) {
            fprintf(stderr, "Put %s fail\n", argv[2]);
            return 2;
        }
    } else if (strcmp(argv[1], "get") == 0 ) {
        std::string value;
        if (!kv_client.Get(argv[2], &value)) {
            fprintf(stderr, "Get %s fail\n", argv[2]);
            return 2;
        }
        printf("Get %s return %s\n", argv[2], value.c_str());
    } else if (strcmp(argv[1], "delete") == 0 ) {
        if (!kv_client.Delete(argv[2])) {
            fprintf(stderr, "Delete %s fail\n", argv[2]);
            return 2;
        }
        printf("Delete %s done\n", argv[2]);
    }
    return 0;
}


