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
    virtual bool GetLeader(std::string* leader_addr) = 0;
    virtual bool Log(int64_t seq, int32_t type, const std::string& key,
                     const std::string& value, bool is_last) = 0;
    virtual int ScanLog() = 0;
    virtual int Next(int64_t* seq, int32_t* type, char* key, char* value) = 0;
};

class MasterSlave : public Sync {
public:
    MasterSlave();
    virtual ~MasterSlave() {};
    virtual bool GetLeader(std::string* leader_addr);
    virtual bool Log(int64_t seq, int32_t type, const std::string& key,
                     const std::string& value, bool is_last);
    virtual int ScanLog();
    virtual int Next(int64_t* seq, int32_t* type, char* key, char* value);
private:
    void EncodeLog(int64_t seq, int32_t type, const std::string& key,
                   const std::string& value, uint32_t encode_len, char* entry);
    void DecodeLog(char* const input, int64_t* seq, int32_t* type, char* key, char* value);
private:
    int log_;
    int scan_log_;
    std::multimap<int64_t, std::string> entries_;
};

} // namespace bfs
} // namespace baidu

#endif  //BFS_NAMESERVER_SYNC_SYNC_H_
