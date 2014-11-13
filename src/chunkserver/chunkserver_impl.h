// Copyright (c) 2014, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#ifndef  BFS_TRUNKSERVER_IMPL_H_
#define  BFS_TRUNKSERVER_IMPL_H_

#include "proto/chunkserver.pb.h"

namespace bfs {


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
private:
    int64_t _chunkserver_id;
    int64_t _namespace_version;
};

} // namespace bfs
#endif  //__TRUNKSERVER_IMPL_H_

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
