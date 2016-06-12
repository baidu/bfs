// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#include "nameserver_client.h"

#include <common/string_util.h>
#include "rpc_client.h"

namespace baidu {
namespace bfs {


NameServerClient::NameServerClient(RpcClient* rpc_client, const std::string& nameserver_nodes)
    : rpc_client_(rpc_client), leader_id_(0) {
    common::SplitString(nameserver_nodes, ",", &nameserver_nodes_);
    stubs_.resize(nameserver_nodes_.size());
    for (uint32_t i = 0; i < nameserver_nodes_.size(); i++) {
        rpc_client_->GetStub(nameserver_nodes_[i], &stubs_[i]);
    }
}

}
}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
