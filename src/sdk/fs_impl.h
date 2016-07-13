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
#include "proto/status_code.pb.h"


namespace baidu {
namespace bfs {

class RpcClient;
class NameServerClient;

int32_t GetErrorCode(baidu::bfs::StatusCode stat);

class FSImpl : public FS {
public:
    friend class FileImpl;
    FSImpl();
    ~FSImpl();
    bool ConnectNameServer(const char* nameserver);
    int32_t CreateDirectory(const char* path);
    int32_t ListDirectory(const char* path, BfsFileInfo** filelist, int *num);
    int32_t DeleteDirectory(const char* path, bool recursive);
    int32_t Access(const char* path, int32_t mode);
    int32_t Stat(const char* path, BfsFileInfo* fileinfo);
    int32_t GetFileSize(const char* path, int64_t* file_size);
    int32_t GetFileLocation(const std::string& path,
                         std::map<int64_t, std::vector<std::string> >* locations);
    int32_t OpenFile(const char* path, int32_t flags, File** file);
    int32_t OpenFile(const char* path, int32_t flags, int32_t mode,
                  int32_t replica, File** file);
    int32_t CloseFile(File* file);
    int32_t DeleteFile(const char* path);
    int32_t Rename(const char* oldpath, const char* newpath);
    int32_t ChangeReplicaNum(const char* file_name, int32_t replica_num);
    int32_t SysStat(const std::string& stat_name, std::string* result);
    int32_t ShutdownChunkServer(const std::vector<std::string>& cs_addr);
    int32_t ShutdownChunkServerStat();
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
