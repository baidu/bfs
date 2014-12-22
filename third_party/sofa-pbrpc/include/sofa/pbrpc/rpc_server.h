// Copyright (c) 2014 Baidu.com, Inc. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: qinzuoyan01@baidu.com (Qin Zuoyan)

#ifndef _SOFA_PBRPC_RPC_SERVER_H_
#define _SOFA_PBRPC_RPC_SERVER_H_

#include <google/protobuf/service.h>

#include <sofa/pbrpc/common.h>
#include <sofa/pbrpc/ext_closure.h>
#include <sofa/pbrpc/rpc_error_code.h>

namespace sofa {
namespace pbrpc {

// Defined in other files.
class RpcServerImpl;

struct RpcServerOptions {
    int work_thread_num; // thread count for network handing and service processing, default 8.

    int keep_alive_time;         // keep alive time of idle connections.
                                 // idle connections will be closed if no read/write for this time.
                                 // in seconds, should >= -1, -1 means for ever, default 65.

    int max_pending_buffer_size; // max buffer size of the pending send queue for one connection.
                                 // in MB, should >= 0, 0 means no buffer, default 2.

    // Network throughput limit.
    // The network bandwidth is shared by all connections:
    // * busy connections get more bandwidth.
    // * the total bandwidth of all connections will not exceed the limit.
    int max_throughput_in;       // max network in throughput for all connections.
                                 // in MB/s, should >= -1, -1 means no limit, default -1.
    int max_throughput_out;      // max network out throughput for all connections.
                                 // in MB/s, should >= -1, -1 means no limit, default -1.

    bool disable_builtin_services; // If disable builtin services, including health service, list 
                                   // service, status service, and so on.  Default false.
    bool disable_list_service;     // If disable the list service, which would public your service
                                   // protobuf descriptor.  Default false.

    // Thread init function called at starting of each work thread.
    // This closure should be a permanent closure created and destroyed by user.
    // Default is NULL, means no init function.
    // Return false if init failed.
    // If any init function returns false, then the server will start failed.
    ExtClosure<bool()>* work_thread_init_func;

    // Thread destroy function called at ending of each work thread (even if init failed).
    // This closure should be a permanent closure created and destroyed by user.
    // Default is NULL, means no destroy function.
    ExtClosure<void()>* work_thread_dest_func;

    RpcServerOptions()
        : work_thread_num(8)
        , keep_alive_time(65)
        , max_pending_buffer_size(2)
        , max_throughput_in(-1)
        , max_throughput_out(-1)
        , disable_builtin_services(false)
        , disable_list_service(false)
        , work_thread_init_func(NULL)
        , work_thread_dest_func(NULL)
    {}
};

class RpcServer
{
public:
    class EventHandler {
    public:
        virtual ~EventHandler() {}

        // This function is used to notify that some error has ocurred when accepting connections.
        // The reason is passed by "error_code" and "error_text".  If needed, the listener may be
        // restarted to recover to usable state.
        //
        // The "error_code" may be:
        //     RPC_ERROR_TOO_MANY_OPEN_FILES
        //     RPC_ERROR_UNKNOWN
        virtual void NotifyAcceptFailed(RpcErrorCode error_code, const std::string& error_text) = 0;
    };

public:
    // Constructor.
    // @param options The rpc server options.
    // @param handler The event handler.  It will be taken overby the rpc server and will be
    //                deleted when the server destroys.
    explicit RpcServer(const RpcServerOptions& options = RpcServerOptions(),
                       EventHandler* handler = NULL);
    virtual ~RpcServer();

    // Start the server, and listen on the "server_address".  If succeed started
    // and listened, return true.
    bool Start(const std::string& server_address);

    // Stop the server.
    void Stop();

    // Wait the server run until catched signal SIGINT or SIGTERM.
    // Returns exit code of process.
    // Example:
    //     int main()
    //     {
    //         ... ...
    //         ::sofa::pbrpc::RpcServer rpc_server;
    //         if (!rpc_server.RegisterService(service)) {
    //             fprintf(stderr, "Register service failed.\n");
    //             return EXIT_FAILURE;
    //         }
    //         if (!rpc_server.Start(address)) {
    //             fprintf(stderr, "Start server failed.\n");
    //             return EXIT_FAILURE;
    //         }
    //         rpc_server.Run();
    //         return EXIT_SUCCESS;
    //     }
    //
    int Run();

    // Get the current rpc server options.
    RpcServerOptions GetOptions();

    // Reset the rpc server options.
    //
    // Current only these options can be reset (others will be ignored):
    // * max_pending_buffer_size : will take effective immediately.
    // * max_throughput_in : will take effective from the next time slice (0.1s). 
    // * max_throughput_out : will take effective from the next time slice (0.1s).
    // * keep_alive_time: will take effective immediately.
    //
    // Though you want to reset only part of these options, the other options also
    // need to be set.  Maybe you can reset by this way:
    //     RpcServerOptions options = rpc_server->GetOptions();
    //     options.max_throughput_in = new_max_throughput_in; // reset some options
    //     rpc_server->ResetOptions(options);
    //
    // The old and new value of reset options will be print to INFO log.
    void ResetOptions(const RpcServerOptions& options);

    // Register a service.  If a service has been registered successfully, and the
    // "take_ownership" is true, the service will be taken overby the rpc server
    // and will be deleted when the server destroys.
    //
    // Services are keyed by service full name, so each service can be registered once.
    // If the service full name has been registered before, return false.
    //
    // Preconditions:
    // * "service" != NULL
    bool RegisterService(google::protobuf::Service* service, bool take_ownership = true);

    // Get the count of current registed services.
    int ServiceCount();

    // Get the count of current alive connections.
    int ConnectionCount();

    // Return true if the server is listening on some address.
    bool IsListening();

public:
    const sofa::pbrpc::shared_ptr<RpcServerImpl>& impl() const
    {
        return _impl;
    }

private:
    sofa::pbrpc::shared_ptr<RpcServerImpl> _impl;

    SOFA_PBRPC_DISALLOW_EVIL_CONSTRUCTORS(RpcServer);
}; // class RpcServer

} // namespace pbrpc
} // namespace sofa

#endif // _SOFA_PBRPC_RPC_SERVER_H_

/* vim: set ts=4 sw=4 sts=4 tw=100 */
