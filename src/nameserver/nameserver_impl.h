// Copyright (c) 2014, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#ifndef  BFS_NAMESERVER_IMPL_H_
#define  BFS_NAMESERVER_IMPL_H_

#include "common/mutex.h"
#include "common/thread_pool.h"
#include "proto/nameserver.pb.h"

namespace sofa {
namespace pbrpc {
class HTTPRequest;
class HTTPResponse;
}
}

namespace bfs {

class NameSpace;
class ChunkServerManager;
class BlockManager;

class NameServerImpl : public NameServer {
public:
    NameServerImpl();
    virtual ~NameServerImpl();
    void CreateFile(::google::protobuf::RpcController* controller,
                       const ::bfs::CreateFileRequest* request,
                       ::bfs::CreateFileResponse* response,
                       ::google::protobuf::Closure* done);
    void AddBlock(::google::protobuf::RpcController* controller,
                       const ::bfs::AddBlockRequest* request,
                       ::bfs::AddBlockResponse* response,
                       ::google::protobuf::Closure* done);
    void GetFileLocation(::google::protobuf::RpcController* controller,
                       const ::bfs::FileLocationRequest* request,
                       ::bfs::FileLocationResponse* response,
                       ::google::protobuf::Closure* done);
    void ListDirectory(::google::protobuf::RpcController* controller,
                       const ::bfs::ListDirectoryRequest* request,
                       ::bfs::ListDirectoryResponse* response,
                       ::google::protobuf::Closure* done);
    void Stat(::google::protobuf::RpcController* controller,
                       const ::bfs::StatRequest* request,
                       ::bfs::StatResponse* response,
                       ::google::protobuf::Closure* done);
    void Rename(::google::protobuf::RpcController* controller,
                       const ::bfs::RenameRequest* request,
                       ::bfs::RenameResponse* response,
                       ::google::protobuf::Closure* done);
    void Unlink(::google::protobuf::RpcController* controller,
                       const ::bfs::UnlinkRequest* request,
                       ::bfs::UnlinkResponse* response,
                       ::google::protobuf::Closure* done);
    void DeleteDirectory(::google::protobuf::RpcController* controller,
                         const ::bfs::DeleteDirectoryRequest* request,
                         ::bfs::DeleteDirectoryResponse* response,
                         ::google::protobuf::Closure* done);
    void FinishBlock(::google::protobuf::RpcController* controller,
                       const ::bfs::FinishBlockRequest* request,
                       ::bfs::FinishBlockResponse* response,
                       ::google::protobuf::Closure* done);
    void ChangeReplicaNum(::google::protobuf::RpcController* controller,
                       const ::bfs::ChangeReplicaNumRequest* request,
                       ::bfs::ChangeReplicaNumResponse* response,
                       ::google::protobuf::Closure* done);
    void HeartBeat(::google::protobuf::RpcController* controller,
                       const ::bfs::HeartBeatRequest* request,
                       ::bfs::HeartBeatResponse* response,
                       ::google::protobuf::Closure* done);
    void BlockReport(::google::protobuf::RpcController* controller,
                       const ::bfs::BlockReportRequest* request,
                       ::bfs::BlockReportResponse* response,
                       ::google::protobuf::Closure* done);
    void PullBlockReport(::google::protobuf::RpcController* controller,
                       const ::bfs::PullBlockReportRequest* request,
                       ::bfs::PullBlockReportResponse* response,
                       ::google::protobuf::Closure* done);
    void SysStat(::google::protobuf::RpcController* controller,
                       const ::bfs::SysStatRequest* request,
                       ::bfs::SysStatResponse* response,
                       ::google::protobuf::Closure* done);
    bool WebService(const sofa::pbrpc::HTTPRequest&, sofa::pbrpc::HTTPResponse&);
private:
    void RebuildBlockMap();
    void LogStatus();
    void LeaveSafemode();
private:
    /// Global thread pool
    ThreadPool _thread_pool;
    /// Global lock
    Mutex        _mu;
    /// Chunkserver map
    ChunkServerManager* _chunkserver_manager;
    /// Block map
    BlockManager* _block_manager;
    bool _safe_mode;
    /// Namespace
    NameSpace* _namespace;
    int64_t _namespace_version;
};

} // namespace bfs

#endif  //BFS_NAMESERVER_IMPL_H_

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
