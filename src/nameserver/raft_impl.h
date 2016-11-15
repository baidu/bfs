// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#ifndef  BFS_RAFT_IMPL_H_
#define  BFS_RAFT_IMPL_H_

#include <string>
#include <functional>

#include "sync.h"

namespace google {
namespace protobuf {
class Service;
}
}
namespace baidu {
namespace bfs {

class RaftNodeImpl;

class RaftImpl : public Sync {
public:
    RaftImpl();
    ~RaftImpl();
    void Init(std::function<void (const std::string& log)> callback);
    bool IsLeader(std::string* leader_addr = NULL);
    bool Log(const std::string& entry, int timeout_ms = 10000);
    void Log(const std::string& entry, std::function<void (bool)> callback);
    void SwitchToLeader() {}
    std::string GetStatus();
public:
    google::protobuf::Service* GetService();
private:
    RaftNodeImpl* raft_node_;
};

}
}
#endif  // BFS_RAFT_IMPL_H_

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
