// Copyright (c) 2014, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#ifndef  BFS_CHUNKSERVER_IMPL_H_
#define  BFS_CHUNKSERVER_IMPL_H_

#include "proto/chunkserver.pb.h"
#include "proto/nameserver.pb.h"
#include "proto/status_code.pb.h"

#include <common/thread_pool.h>

namespace sofa {
namespace pbrpc {
struct HTTPRequest;
struct HTTPResponse;
}
}

namespace baidu {
namespace bfs {

class BlockManager;
class RpcClient;
class NameServerClient;
class ChunkServer_Stub;
class Block;
class CounterManager;

class ChunkServerImpl : public ChunkServer {
public:
    ChunkServerImpl();
    virtual ~ChunkServerImpl();

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
    bool WebService(const sofa::pbrpc::HTTPRequest& request,
                    sofa::pbrpc::HTTPResponse& response);
private:
    void LogStatus(bool routine);
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
    void PushBlock(const ReplicaInfo& new_replica_info, int32_t cancel_time);
    StatusCode PushBlockProcess(const ReplicaInfo& new_replica_info, int32_t cancel_time);
    StatusCode WriteRecoverBlock(Block* block, ChunkServer_Stub* chunkserver, int32_t cancel_time, bool* timeout);
    void CloseIncompleteBlock(int64_t block_id);
    void StopBlockReport();
    void SendHeartbeat();
    void SendBlockReport();
    void Register();
    bool ReportFinish(Block* block);
private:
    BlockManager*   block_manager_;
    std::string     data_server_addr_;
    RpcClient*      rpc_client_;
    ThreadPool*     work_thread_pool_;
    ThreadPool*     read_thread_pool_;
    ThreadPool*     write_thread_pool_;
    ThreadPool*     recover_thread_pool_;
    ThreadPool*     heartbeat_thread_;
    NameServerClient* nameserver_;
    int32_t chunkserver_id_;
    CounterManager* counter_manager_;
    int64_t heartbeat_task_id_;
    volatile int64_t blockreport_task_id_;
    int64_t last_report_blockid_;
    int64_t report_id_;
    bool is_first_round_;
    int64_t first_round_report_start_;
    volatile bool service_stop_;

    Params params_;
};

} // namespace bfs
} // namespace baidu

#endif  //__CHUNKSERVER_IMPL_H_

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
