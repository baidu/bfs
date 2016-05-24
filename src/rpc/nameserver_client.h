// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#ifndef  BFS_NAMESERVER_CLIENT_H_
#define  BFS_NAMESERVER_CLIENT_H_

#include <string>
#include <vector>

#include <sofa/pbrpc/pbrpc.h>

#include <common/mutex.h>
#include <common/logging.h>

#include "rpc_client.h"
#include "proto/status_code.pb.h"
#include "proto/nameserver.pb.h"

namespace baidu {
namespace bfs {

class RpcClient;
class NameServer_Stub;
class NameServerClient {
public:
    NameServerClient(RpcClient* rpc_client, const std::string& nameserver_nodes);

    template <class Request, class Response, class Callback>
    bool SendRequest(void(NameServer_Stub::*func)(google::protobuf::RpcController*,
                                                  const Request*, Response*, Callback*),
                     const Request* request, Response* response,
                     int32_t rpc_timeout, int retry_times = 1) {
        bool ret = false;
        for (uint32_t i = 0; i < stubs_.size(); i++) {
            int ns_id = leader_id_;
            ret = rpc_client_->SendRequest(stubs_[ns_id], func, request, response,
                                           rpc_timeout, retry_times);
            if (ret && response->status() != kIsFollower) {
            //    LOG(DEBUG, "Send rpc to %d %s return %s", 
            //        leader_id_, nameserver_nodes_[leader_id_].c_str(),
            //        StatusCode_Name(response->status()).c_str());
                return true;
            }
            MutexLock lock(&mu_);
            if (ns_id == leader_id_) {
                leader_id_ = (leader_id_ + 1) % stubs_.size();
            }
            //LOG(INFO, "Try next nameserver %d %s",
            //    leader_id_, nameserver_nodes_[leader_id_].c_str());
        }
        return ret;
    }
private:
    RpcClient* rpc_client_;
    std::vector<std::string> nameserver_nodes_;
    std::vector<NameServer_Stub*> stubs_;
    Mutex mu_;
    int leader_id_;
};

}
}
#endif  //__NAMESERVER_CLIENT_H_

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
