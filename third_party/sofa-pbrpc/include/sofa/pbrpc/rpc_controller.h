// Copyright (c) 2014 Baidu.com, Inc. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: qinzuoyan01@baidu.com (Qin Zuoyan)

#ifndef _SOFA_PBRPC_RPC_CONTROLLER_H_
#define _SOFA_PBRPC_RPC_CONTROLLER_H_

#include <google/protobuf/service.h>

#include <sofa/pbrpc/common.h>
#include <sofa/pbrpc/rpc_option.pb.h>

namespace sofa {
namespace pbrpc {

class RpcControllerImpl;

class RpcController : public google::protobuf::RpcController
{
public:
    RpcController();
    virtual ~RpcController();

    // -------- used by both client and server side ---------
    // These calls can be made from both client side and server side.
 
    // Get the local address in format of "ip:port".
    //
    // For client:
    // This method can only be called after the call has finished.  If 
    // IsRequestSent() is true, returns the local address used for sending
    // the message; else, the return value is undefined.
    //
    // For server:
    // This method returns the local address where the message received from.
    std::string LocalAddress() const;

    // Get the remote address in format of "ip:port".
    //
    // For client:
    // This method returns the remote address where the messsage sent to.
    //
    // For server:
    // This method returns the remote address where the message received from.
    std::string RemoteAddress() const;

    // -------- used only by client side ---------
    // These calls should be made from the client side only.  Their results are
    // undefined on the server side (may crash).
    
    // Resets the RpcController to its initial state so that it may be reused
    // in a new call. Must not be called while it is in use.
    virtual void Reset();

    // Set expect timeout in milli-seconds of the call.  If timeout is not set
    // or set no more than 0, actual timeout will be taken from proto options.
    void SetTimeout(int64 timeout_in_ms);

    // Get the actual timeout in milli-seconds.
    //
    // The actual timeout takes effect in the following order:
    // * set in RpcController (take effective only when timeout > 0)
    // * set in the method proto options (default not set)
    // * set in the service proto options (default value is 10 seconds)
    int64 Timeout() const;

    // Set compress type of the request message.
    // Supported types:
    //   CompressTypeNone
    //   CompressTypeGzip
    //   CompressTypeZlib
    //   CompressTypeSnappy
    //   CompressTypeLZ4
    void SetRequestCompressType(CompressType compress_type);

    // Set expected compress type of the response message.
    // Supported types:
    //   CompressTypeNone
    //   CompressTypeGzip
    //   CompressTypeZlib
    //   CompressTypeSnappy
    //   CompressTypeLZ4
    void SetResponseCompressType(CompressType compress_type);

    // After a call has finished, returns true if the call failed.  The possible
    // reasons for failure depend on the RPC implementation.  Failed() must not
    // be called before a call has finished.  If Failed() returns true, the
    // contents of the response message are undefined.
    //
    // This method can only be called after the call has finished.
    virtual bool Failed() const;

    // If Failed() is true, returns error code which identities the reason.
    // The error code is of type RpcErrorCode.
    //
    // This method can only be called after the call has finished.
    virtual int ErrorCode() const;

    // If Failed() is true, returns a human-readable description of the error.
    // This can only be called after the call has finished.
    //
    // This method can only be called after the call has finished.
    virtual std::string ErrorText() const;

    // If the request has already been set to the remote server, returns true;
    // otherwise returns false.
    //
    // This method can only be called after the call has finished.
    bool IsRequestSent() const;
    
    // If IsRequestSent() is true, returns sent bytes, including the rpc header.
    //
    // This method can only be called after the call has finished.
    int64 SentBytes() const;

    // Advises the RPC system that the caller desires that the RPC call be
    // canceled.  The RPC system may cancel it immediately, may wait awhile and
    // then cancel it, or may not even cancel the call at all.  If the call is
    // canceled, the "done" callback will still be called and the RpcController
    // will indicate that the call failed at that time.
    //
    // Not supported now.
    virtual void StartCancel();

    // -------- used only by server side ---------
    // These calls should be made from the server side only.  Their results
    // are undefined on the client side (may crash).

    // Causes Failed() to return true on the client side.  "reason" will be
    // incorporated into the message returned by ErrorText().  If you find
    // you need to return machine-readable information about failures, you
    // should incorporate it into your response protocol buffer and should
    // NOT call SetFailed().
    virtual void SetFailed(const std::string& reason);

    // If true, indicates that the client canceled the RPC, so the server may
    // as well give up on replying to it.  The server should still call the
    // final "done" callback.
    virtual bool IsCanceled() const;

    // Asks that the given callback be called when the RPC is canceled.  The
    // callback will always be called exactly once.  If the RPC completes without
    // being canceled, the callback will be called after completion.  If the RPC
    // has already been canceled when NotifyOnCancel() is called, the callback
    // will be called immediately.
    //
    // NotifyOnCancel() must be called no more than once per request.
    virtual void NotifyOnCancel(google::protobuf::Closure* callback);

public:
    const sofa::pbrpc::shared_ptr<RpcControllerImpl>& impl() const
    {
        return _impl;
    }

private:
    sofa::pbrpc::shared_ptr<RpcControllerImpl> _impl;

    SOFA_PBRPC_DISALLOW_EVIL_CONSTRUCTORS(RpcController);
}; // class RpcController

} // namespace pbrpc
} // namespace sofa

#endif // _SOFA_PBRPC_RPC_CONTROLLER_H_

/* vim: set ts=4 sw=4 sts=4 tw=100 */
