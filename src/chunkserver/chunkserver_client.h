// Copyright (c) 2014, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#ifndef  BFS_TRUNKSERVER_CLIENT_H_
#define  BFS_TRUNKSERVER_CLIENT_H_

#include "rpc/rpc_client.h"
#include "proto/chunkserver.pb.h"

namespace bfs {

class ChunkServerClient {
public:
    explicit ChunkServerClient(RpcClient* rpc_client, const std::string addr) 
      : _rpc_client(rpc_client), _stub(NULL) {
        _rpc_client->GetStub(addr, &_stub);
    }
    ~ChunkServerClient() {
        delete _stub;
        _stub = NULL;
    }
    template <class Stub, class Request, class Response, class Callback>
    bool SendRequest(void(Stub::*func)(
                    google::protobuf::RpcController*,
                    const Request*, Response*, Callback*),
                    const Request* request, Response* response,
                    int32_t rpc_timeout, int retry_times) {
         return _rpc_client->SendRequest(_stub, func, request, response, rpc_timeout, retry_times);
    }
private:
    ChunkServerClient(const ChunkServerClient&);
    ChunkServerClient& operator=(const ChunkServerClient&);
    RpcClient* _rpc_client;
    ChunkServer_Stub* _stub;
};

} // namespace bfs

#endif

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
