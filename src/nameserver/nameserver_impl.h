// Copyright (c) 2014, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#ifndef  BFS_NAMESERVER_IMPL_H_
#define  BFS_NAMESERVER_IMPL_H_

#include <common/thread_pool.h>

#include "proto/nameserver.pb.h"

namespace sofa {
namespace pbrpc {
class HTTPRequest;
class HTTPResponse;
}
}

namespace baidu {
namespace bfs {

class NameSpace;
class ChunkServerManager;
class BlockMapping;
class Sync;

class NameServerImpl : public NameServer {
public:
    NameServerImpl(Sync* sync);
    virtual ~NameServerImpl();
    void CreateFile(::google::protobuf::RpcController* controller,
                       const CreateFileRequest* request,
                       CreateFileResponse* response,
                       ::google::protobuf::Closure* done);
    void AddBlock(::google::protobuf::RpcController* controller,
                       const AddBlockRequest* request,
                       AddBlockResponse* response,
                       ::google::protobuf::Closure* done);
    void GetFileLocation(::google::protobuf::RpcController* controller,
                       const FileLocationRequest* request,
                       FileLocationResponse* response,
                       ::google::protobuf::Closure* done);
    void ListDirectory(::google::protobuf::RpcController* controller,
                       const ListDirectoryRequest* request,
                       ListDirectoryResponse* response,
                       ::google::protobuf::Closure* done);
    void Stat(::google::protobuf::RpcController* controller,
                       const StatRequest* request,
                       StatResponse* response,
                       ::google::protobuf::Closure* done);
    void Rename(::google::protobuf::RpcController* controller,
                       const RenameRequest* request,
                       RenameResponse* response,
                       ::google::protobuf::Closure* done);
    void Unlink(::google::protobuf::RpcController* controller,
                       const UnlinkRequest* request,
                       UnlinkResponse* response,
                       ::google::protobuf::Closure* done);
    void DeleteDirectory(::google::protobuf::RpcController* controller,
                         const DeleteDirectoryRequest* request,
                         DeleteDirectoryResponse* response,
                         ::google::protobuf::Closure* done);
    void FinishBlock(::google::protobuf::RpcController* controller,
                       const FinishBlockRequest* request,
                       FinishBlockResponse* response,
                       ::google::protobuf::Closure* done);
    void ChangeReplicaNum(::google::protobuf::RpcController* controller,
                       const ChangeReplicaNumRequest* request,
                       ChangeReplicaNumResponse* response,
                       ::google::protobuf::Closure* done);
    void HeartBeat(::google::protobuf::RpcController* controller,
                       const HeartBeatRequest* request,
                       HeartBeatResponse* response,
                       ::google::protobuf::Closure* done);
    void Register(::google::protobuf::RpcController* controller,
                       const ::baidu::bfs::RegisterRequest* request,
                       ::baidu::bfs::RegisterResponse* response,
                       ::google::protobuf::Closure* done);
    void BlockReport(::google::protobuf::RpcController* controller,
                       const BlockReportRequest* request,
                       BlockReportResponse* response,
                       ::google::protobuf::Closure* done);
    void BlockReceived(::google::protobuf::RpcController* controller,
                       const BlockReceivedRequest* request,
                       BlockReceivedResponse* response,
                       ::google::protobuf::Closure* done);
    void PushBlockReport(::google::protobuf::RpcController* controller,
                       const PushBlockReportRequest* request,
                       PushBlockReportResponse* response,
                       ::google::protobuf::Closure* done);
    void SysStat(::google::protobuf::RpcController* controller,
                       const SysStatRequest* request,
                       SysStatResponse* response,
                       ::google::protobuf::Closure* done);

    bool WebService(const sofa::pbrpc::HTTPRequest&, sofa::pbrpc::HTTPResponse&);

private:
    void CheckLeader();
    void RebuildBlockMapCallback(const FileInfo& file_info);
    void LogStatus();
    void Register();
    void CheckSafemode();
    void LeaveSafemode();
    void ListRecover(sofa::pbrpc::HTTPResponse* response);
    bool LogRemote(const NameServerLog& log, boost::function<void (bool)> callback);
    void SyncLogCallback(::google::protobuf::RpcController* controller,
                         const ::google::protobuf::Message* request,
                         ::google::protobuf::Message* response,
                         ::google::protobuf::Closure* done,
                         std::vector<FileInfo>* removed,
                         bool ret);
private:
    /// Global thread pool
    ThreadPool* work_thread_pool_;
    ThreadPool* report_thread_pool_;
    /// Chunkserver map
    ChunkServerManager* chunkserver_manager_;
    /// Block map
    BlockMapping* block_mapping_;
    /// Safemode
    volatile int safe_mode_;
    int64_t start_time_;
    /// Namespace
    NameSpace* namespace_;
    /// ha
    Sync* sync_;
    bool is_leader_;
};

} // namespace bfs
} // namespace biadu

#endif  //BFS_NAMESERVER_IMPL_H_

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
