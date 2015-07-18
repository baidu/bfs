// Copyright (c) 2014, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#ifndef  BFS_TRUNKSERVER_IMPL_H_
#define  BFS_TRUNKSERVER_IMPL_H_

#include "proto/chunkserver.pb.h"
#include "proto/nameserver.pb.h"
#include "common/thread_pool.h"

namespace bfs {

class BlockManager;
class RpcClient;
class NameServer_Stub;
class ChunkServer_Stub;
class Block;

class ChunkServerImpl : public ChunkServer {
public:
    ChunkServerImpl();
    virtual ~ChunkServerImpl();
    static void* RoutineWrapper(void* arg);
    void Routine();
    bool ReportFinish(Block* block);
    
    virtual void WriteBlock(::google::protobuf::RpcController* controller,
                            const WriteBlockRequest* request,
                            WriteBlockResponse* response,
                            ::google::protobuf::Closure* done);
    virtual void ReadBlock(::google::protobuf::RpcController* controller,
                           const ReadBlockRequest* request,
                           ReadBlockResponse* response,
                           ::google::protobuf::Closure* done);
    virtual void GetBlockInfo(::google::protobuf::RpcController* controller,
                              const GetBlockInfoRequest* request,
                              GetBlockInfoResponse* response,
                              ::google::protobuf::Closure* done);
private:
    void LogStatus();
    void WriteNext(const std::string& next_server,
                   ChunkServer_Stub* stub,
                   const WriteBlockRequest* next_request,
                   WriteBlockResponse* next_response,
                   const WriteBlockRequest* request,
                   WriteBlockResponse* response,
                   ::google::protobuf::Closure* done);
    void WriteNextCallback(const WriteBlockRequest* next_request,
                           WriteBlockResponse* next_response,
                           bool failed, int error,
                           const std::string& next_server,
                           std::pair<const WriteBlockRequest*, WriteBlockResponse*> origin,
                           ::google::protobuf::Closure* done,
                           ChunkServer_Stub* stub);
    void LocalWriteBlock(const WriteBlockRequest* request,
                         WriteBlockResponse* response,
                         ::google::protobuf::Closure* done);
    void RemoveObsoleteBlocks(std::vector<int64_t> blocks);
    void PullNewBlocks(std::vector<ReplicaInfo> new_replica_info);
private:
    BlockManager*   _block_manager;
    std::string     _data_server_addr;
    RpcClient*      _rpc_client;
    ThreadPool*     _work_thread_pool;
    ThreadPool*     _read_thread_pool;
    ThreadPool*     _write_thread_pool;
    NameServer_Stub* _nameserver;
    pthread_t _routine_thread;
    bool _quit;
    int32_t _chunkserver_id;
    int64_t _namespace_version;
};

} // namespace bfs

#endif  //__TRUNKSERVER_IMPL_H_

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
