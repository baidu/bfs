// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//

#ifndef  BFS_NAMESERVER_SYNC_SYNC_H_
#define  BFS_NAMESERVER_SYNC_SYNC_H_

#include <string>
#include <map>

#include "proto/status_code.pb.h"

namespace baidu {
namespace bfs {

class Sync {
public:
    virtual ~Sync() {}
    virtual void Init() = 0;
    virtual bool GetLeader(std::string* leader_addr) = 0;
    virtual bool Log(const std::string& entry) = 0;
    virtual int ScanLog() = 0;
    virtual int Next(char* entry) = 0;
};

class MasterSlave : public Sync {
public:
    MasterSlave();
    virtual ~MasterSlave() {};
    virtual void Init();
    virtual bool GetLeader(std::string* leader_addr);
    virtual bool Log(const std::string& entry);
    virtual int ScanLog();
    virtual int Next(char* entry);
private:
    int log_;
    int scan_log_;
    std::multimap<int64_t, std::string> entries_;
};

} // namespace bfs
} // namespace baidu

#endif  //BFS_NAMESERVER_SYNC_SYNC_H_
