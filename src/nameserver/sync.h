// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//

#ifndef  BFS_NAMESERVER_SYNC_H_
#define  BFS_NAMESERVER_SYNC_H_

#include <string>
#include <boost/function.hpp>

#include "proto/status_code.pb.h"

namespace baidu {
namespace bfs {

class RpcClient;

class Sync {
public:
    virtual ~Sync() {}
    // Description: Register 'callback' to Sync and redo log.
    // NOTICE: Sync does not work until Init is called.
    //virtual void Init(boost::function<void (const std::string& log)> callback) = 0;
    virtual void Init(boost::function<void (const std::string& log, int64_t)> callback) = 0;
    // Description: Return true if this server is Leader.
    // TODO: return 'leader_addr' which points to the current leader.
    virtual bool IsLeader(std::string* leader_addr = NULL) = 0;
    // Description: Synchronous interface. Leader will replicate 'entry' to followers.
    // Return true upon success.
    // Follower will ignore this call and return true
    virtual bool Log(const std::string& entry, int timeout_ms = 10000) = 0;
    // Description: Asynchronous interface. Leader will replicate 'entry' to followers,
    // then call 'callback' with result(true if success, false is failed) to notify the user.
    // Follower will ignore this call and return true.
    virtual void Log(const std::string& entry, boost::function<void (int64_t)> callback) = 0;
    // Turn a follower to leader.
    // Leader will ignore this call.
    virtual void SwitchToLeader() = 0;
};

} // namespace bfs
} // namespace baidu

#endif  //BFS_NAMESERVER_SYNC_SYNC_H_
