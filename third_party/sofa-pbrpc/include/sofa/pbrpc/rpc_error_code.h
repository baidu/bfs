// Copyright (c) 2014 Baidu.com, Inc. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: qinzuoyan01@baidu.com (Qin Zuoyan)

#ifndef _SOFA_PBRPC_RPC_ERROR_CODE_H_
#define _SOFA_PBRPC_RPC_ERROR_CODE_H_

namespace sofa {
namespace pbrpc {

enum RpcErrorCode {
    RPC_SUCCESS = 0,
    RPC_ERROR_PARSE_REQUEST_MESSAGE = 1,
    RPC_ERROR_PARSE_RESPONSE_MESSAGE = 2,
    RPC_ERROR_UNCOMPRESS_MESSAGE = 3,
    RPC_ERROR_COMPRESS_TYPE = 4,
    RPC_ERROR_NOT_SPECIFY_METHOD_NAME = 5,
    RPC_ERROR_PARSE_METHOD_NAME = 6,
    RPC_ERROR_FOUND_SERVICE = 7,
    RPC_ERROR_FOUND_METHOD = 8,
    RPC_ERROR_CHANNEL_BROKEN = 9,
    RPC_ERROR_CONNECTION_CLOSED = 10,
    RPC_ERROR_REQUEST_TIMEOUT = 11, // request timeout
    RPC_ERROR_REQUEST_CANCELED = 12, // request canceled
    RPC_ERROR_SERVER_UNAVAILABLE = 13, // server un-healthy
    RPC_ERROR_SERVER_UNREACHABLE = 14, // server un-reachable
    RPC_ERROR_SERVER_SHUTDOWN = 15,
    RPC_ERROR_SEND_BUFFER_FULL = 16,
    RPC_ERROR_SERIALIZE_REQUEST = 17,
    RPC_ERROR_SERIALIZE_RESPONSE = 18,
    RPC_ERROR_RESOLVE_ADDRESS = 19,
    RPC_ERROR_CREATE_STREAM = 20,
    RPC_ERROR_NOT_IN_RUNNING = 21,

    // error code for listener
    RPC_ERROR_TOO_MANY_OPEN_FILES = 101,

    RPC_ERROR_UNKNOWN = 999,
    RPC_ERROR_FROM_USER = 1000,
}; // enum RpcErrorCode

// Convert rpc error code to human readable string.
const char* RpcErrorCodeToString(int error_code);

} // namespace pbrpc
} // namespace sofa

#endif // _SOFA_PBRPC_RPC_ERROR_CODE_H_

/* vim: set ts=4 sw=4 sts=4 tw=100 */
