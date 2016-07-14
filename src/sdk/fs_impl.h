// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//

#ifndef  BFS_SDK_FS_IMPL_H_
#define  BFS_SDK_FS_IMPL_H_

#include <vector>
#include <string>

#include <common/thread_pool.h>

#include "bfs.h"

namespace baidu {
namespace bfs {

class RpcClient;
class NameServerClient;

class FSImpl : public FS {
public:
    friend class FileImpl;
    FSImpl();
    ~FSImpl();
    bool ConnectNameServer(const char* nameserver);
    bool CreateDirectory(const char* path);
    bool ListDirectory(const char* path, BfsFileInfo** filelist, int *num);
    bool DeleteDirectory(const char* path, bool recursive);
    bool Access(const char* path, int32_t mode);
    bool Stat(const char* path, BfsFileInfo* fileinfo);
    bool GetFileSize(const char* path, int64_t* file_size);
    bool GetFileLocation(const std::string& path,
                         std::map<int64_t, std::vector<std::string> >* locations);
    bool OpenFile(const char* path, int32_t flags, File** file);
    bool OpenFile(const char* path, int32_t flags, int32_t mode,
                  int32_t replica, File** file);
    bool CloseFile(File* file);
    bool DeleteFile(const char* path);
    bool Rename(const char* oldpath, const char* newpath);
    bool ChangeReplicaNum(const char* file_name, int32_t replica_num);
    bool SysStat(const std::string& stat_name, std::string* result);
    bool ShutdownChunkServer(const std::vector<std::string>& cs_addr);
    int ShutdownChunkServerStat(std::vector<std::string> *cs);
private:
    RpcClient* rpc_client_;
    NameServerClient* nameserver_client_;
    //NameServer_Stub* nameserver_;
    std::vector<std::string> nameserver_addresses_;
    int32_t leader_nameserver_idx_;
    //std::string nameserver_address_;
    std::string local_host_name_;
    ThreadPool* thread_pool_;
};

} // namespace bfs
} // namespace baidu

#endif  // BFS_SDK_FS_IMPL_H_
