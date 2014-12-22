// Copyright (c) 2014 Baidu.com, Inc. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: qinzuoyan01@baidu.com (Qin Zuoyan)

#ifndef _SOFA_PBRPC_RPC_CLIENT_H_
#define _SOFA_PBRPC_RPC_CLIENT_H_

#include <sofa/pbrpc/common.h>

namespace sofa {
namespace pbrpc {

// Defined in other files.
class RpcClientImpl;

struct RpcClientOptions {
    int work_thread_num;         // num of threads used for network handing, default 4.
    int callback_thread_num;     // num of threads used for async callback, default 4.

    int keep_alive_time;         // keep alive time of idle connections.
                                 // idle connections will be closed if no read/write for this time.
                                 // in seconds, should >= -1, -1 means for ever, default 65.

    int max_pending_buffer_size; // max buffer size of the pending send queue for each connection.
                                 // in MB, should >= 0, 0 means no buffer, default 2.

    // Network throughput limit.
    // The network bandwidth is shared by all connections:
    // * busy connections get more bandwidth.
    // * the total bandwidth of all connections will not exceed the limit.
    int max_throughput_in;       // max network in throughput for all connections.
                                 // in MB/s, should >= -1, -1 means no limit, default -1.
    int max_throughput_out;      // max network out throughput for all connections.
                                 // in MB/s, should >= -1, -1 means no limit, default -1.

    RpcClientOptions()
        : work_thread_num(4)
        , callback_thread_num(4)
        , keep_alive_time(65)
        , max_pending_buffer_size(2)
        , max_throughput_in(-1)
        , max_throughput_out(-1)
    {}
};

class RpcClient
{
public:
    explicit RpcClient(const RpcClientOptions& options = RpcClientOptions());
    virtual ~RpcClient();

    // Get the current rpc client options.
    RpcClientOptions GetOptions();

    // Reset the rpc client options.
    //
    // Current only these options can be reset (others will be ignored):
    // * max_pending_buffer_size : will take effective immediately.
    // * max_throughput_in : will take effective from the next time slice (0.1s). 
    // * max_throughput_out : will take effective from the next time slice (0.1s).
    //
    // Though you want to reset only part of these options, the other options also
    // need to be set.  Maybe you can reset by this way:
    //     RpcClientOptions options = rpc_client->GetOptions();
    //     options.max_throughput_in = new_max_throughput_in; // reset some options
    //     rpc_client->ResetOptions(options);
    //
    // The old and new value of reset options will be print to INFO log.
    void ResetOptions(const RpcClientOptions& options);

    // Get the count of current alive connections.
    int ConnectionCount();

    // Shutdown the rpc client.
    void Shutdown();

public:
    const sofa::pbrpc::shared_ptr<RpcClientImpl>& impl() const
    {
        return _impl;
    }

private:
    sofa::pbrpc::shared_ptr<RpcClientImpl> _impl;

    SOFA_PBRPC_DISALLOW_EVIL_CONSTRUCTORS(RpcClient);
}; // class RpcClient

} // namespace pbrpc
} // namespace sofa

#endif // _SOFA_PBRPC_RPC_CLIENT_H_

/* vim: set ts=4 sw=4 sts=4 tw=100 */
