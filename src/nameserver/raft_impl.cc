// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#include "raft_impl.h"
#include "raft_node.h"

#include <gflags/gflags.h>

DECLARE_string(nameserver_nodes);
DECLARE_int32(node_index);
DECLARE_string(raftdb_path);

namespace baidu {
namespace bfs {

RaftImpl::RaftImpl() {
    raft_node_ = new RaftNodeImpl(FLAGS_nameserver_nodes, FLAGS_node_index, FLAGS_raftdb_path);
}

RaftImpl::~RaftImpl() {
    delete raft_node_;
}

bool RaftImpl::IsLeader(std::string* leader_addr) {
    std::string leader;
    bool ret = raft_node_->GetLeader(&leader);
    if (leader_addr && ret) {
        *leader_addr = leader;
    }
    return ret;
}

bool RaftImpl::Log(const std::string& entry, int timeout_ms) {
    return raft_node_->AppendLog(entry, timeout_ms);
}

void RaftImpl::Log(const std::string& entry, std::function<void (bool)> callback) {
    raft_node_->AppendLog(entry, callback);
}

void RaftImpl::Init(std::function<void (const std::string& log)> callback) {
    return raft_node_->Init(callback);
}

std::string RaftImpl::GetStatus() {
    if (IsLeader(NULL)) {
        return "Raft-leader";
    } else {
        return "Raft-follower";
    }
}

google::protobuf::Service* RaftImpl::GetService() {
    return raft_node_;
}
}
}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
