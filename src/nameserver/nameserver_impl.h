// Copyright (c) 2014, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#ifndef  BFS_NAMESERVER_IMPL_H_
#define  BFS_NAMESERVER_IMPL_H_

#include <common/mutex.h>
#include "proto/nameserver.pb.h"

namespace leveldb {
class DB;
}
namespace bfs {

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
    void FinishBlock(::google::protobuf::RpcController* controller,
                       const ::bfs::FinishBlockRequest* request,
                       ::bfs::FinishBlockResponse* response,
                       ::google::protobuf::Closure* done);
    void HeartBeat(::google::protobuf::RpcController* controller,
                       const ::bfs::HeartBeatRequest* request,
                       ::bfs::HeartBeatResponse* response,
                       ::google::protobuf::Closure* done);
    void BlockReport(::google::protobuf::RpcController* controller,
                       const ::bfs::BlockReportRequest* request,
                       ::bfs::BlockReportResponse* response,
                       ::google::protobuf::Closure* done);

private:
    ChunkServerManager* _chunkserver_manager;
    BlockManager* _block_manager;
    Mutex        _mu;
    leveldb::DB* _db;    ///< ´æ´¢nameserverÊý¾Ý
    int64_t _namespace_version;
};

}

#endif  //BFS_NAMESERVER_IMPL_H_

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
