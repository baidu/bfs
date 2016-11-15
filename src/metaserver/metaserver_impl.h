// Copyright (c) 2014, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#ifndef  BFS_METASERVER_IMPL_H_
#define  BFS_METASERVER_IMPL_H_

#include <common/thread_pool.h>

#include "proto/nameserver.pb.h"
#include "proto/metaserver.pb.h"

namespace sofa {
namespace pbrpc {
struct HTTPRequest;
struct HTTPResponse;
}
}

namespace baidu {
namespace bfs {

class BlockMappingManager;
class ChunkServerManager;
namespace metaserver {

enum RecoverMode {
    kStopRecover = 0,
    kHiOnly = 1,
    kRecoverAll = 2,
};

enum DisplayMode {
    kDisplayAll = 0,
    kAliveOnly = 1,
    kDeadOnly = 2,
    kOverload = 3,
};

class MetaServerImpl : public MetaServer {
public:
    MetaServerImpl();
    virtual ~MetaServerImpl();
    void AddBlock(::google::protobuf::RpcController* controller,
                       const AddBlockRequest* request,
                       AddBlockResponse* response,
                       ::google::protobuf::Closure* done);
    void SyncBlock(::google::protobuf::RpcController* controller,
                       const SyncBlockRequest* request,
                       SyncBlockResponse* response,
                       ::google::protobuf::Closure* done);
    void FinishBlock(::google::protobuf::RpcController* controller,
                       const FinishBlockRequest* request,
                       FinishBlockResponse* response,
                       ::google::protobuf::Closure* done);
    void Register(::google::protobuf::RpcController* controller,
                       const ::baidu::bfs::RegisterRequest* request,
                       ::baidu::bfs::RegisterResponse* response,
                       ::google::protobuf::Closure* done);
    void HeartBeat(::google::protobuf::RpcController* controller,
                             const HeartBeatRequest* request,
                             HeartBeatResponse* response,
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

private:
    void Register();
    void CheckRecoverMode();
    void LeaveReadOnly();
    void ListRecover(sofa::pbrpc::HTTPResponse* response);
    void TransToString(const std::map<int32_t, std::set<int64_t> >& chk_set,
                       std::string* output);
    void TransToString(const std::set<int64_t>& block_set, std::string* output);
    bool CheckFileHasBlock(const FileInfo& file_info,
                           const std::string& file_name,
                           int64_t block_id);
private:
    ThreadPool* read_thread_pool_;
    ThreadPool* work_thread_pool_;
    ThreadPool* report_thread_pool_;
    ThreadPool* heartbeat_thread_pool_;
    ChunkServerManager* chunkserver_manager_;
    BlockMappingManager* block_mapping_manager_;

    volatile bool readonly_;
    volatile int recover_timeout_;
    RecoverMode recover_mode_;
    int64_t start_time_;
};

} // namespace metaserver
} // namespace bfs
} // namespace biadu

#endif  //BFS_METASERVER_IMPL_H_

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
