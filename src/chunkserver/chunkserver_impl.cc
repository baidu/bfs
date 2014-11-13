// Copyright (c) 2014, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>

#include <leveldb/db.h>
#include <leveldb/cache.h>

#include "chunkserver_impl.h"
#include "proto/nameserver.pb.h"
#include "common/mutex.h"

extern std::string FLAGS_block_store_path;
extern std::string FLAGS_nameserver;
extern std::string FLAGS_chunkserver_port;
extern int32_t FLAGS_heartbeat_interval;
extern int32_t FLAGS_blockreport_interval;

namespace bfs {

ChunkServerImpl::ChunkServerImpl()
    : _chunkserver_id(0), _namespace_version(0) {
}

ChunkServerImpl::~ChunkServerImpl() {
}

void ChunkServerImpl::WriteBlock(::google::protobuf::RpcController* controller,
                        const WriteBlockRequest* request,
                        WriteBlockResponse* response,
                        ::google::protobuf::Closure* done) {
    response->set_sequence_id(request->sequence_id());
    done->Run();
}
void ChunkServerImpl::ReadBlock(::google::protobuf::RpcController* controller,
                        const ReadBlockRequest* request,
                        ReadBlockResponse* response,
                        ::google::protobuf::Closure* done) {
    int64_t block_id = request->block_id();
    int64_t offset = request->offset();
    int32_t read_len = request->read_len();

    response->set_status(0);
    done->Run();
}

} // namespace bfs


/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
