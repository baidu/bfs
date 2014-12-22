// Copyright (c) 2014 Baidu.com, Inc. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: qinzuoyan01@baidu.com (Qin Zuoyan)

#ifndef _SOFA_PBRPC_COMMON_H_
#define _SOFA_PBRPC_COMMON_H_

#include <google/protobuf/stubs/common.h>
#include <sofa/pbrpc/smart_ptr/smart_ptr.hpp>

namespace std {}

namespace sofa {
namespace pbrpc {

/////////////// types /////////////
typedef ::google::protobuf::uint uint;

typedef ::google::protobuf::int8  int8;
typedef ::google::protobuf::int16 int16;
typedef ::google::protobuf::int32 int32;
typedef ::google::protobuf::int64 int64;

typedef ::google::protobuf::uint8  uint8;
typedef ::google::protobuf::uint16 uint16;
typedef ::google::protobuf::uint32 uint32;
typedef ::google::protobuf::uint64 uint64;

static const int32 kint32max = ::google::protobuf::kint32max;
static const int32 kint32min = ::google::protobuf::kint32min;
static const int64 kint64max = ::google::protobuf::kint64max;
static const int64 kint64min = ::google::protobuf::kint64min;
static const uint32 kuint32max = ::google::protobuf::kuint32max;
static const uint64 kuint64max = ::google::protobuf::kuint64max;

/////////////// util macros /////////////
#define SOFA_PBRPC_PP_CAT(a, b) SOFA_PBRPC_PP_CAT_I(a, b)
#define SOFA_PBRPC_PP_CAT_I(a, b) a ## b

#define SOFA_PBRPC_DISALLOW_EVIL_CONSTRUCTORS(TypeName)    \
    TypeName(const TypeName&);                       \
    void operator=(const TypeName&)

/////////////// logging and check /////////////
// default log level: ERROR
enum LogLevel {
    LOG_LEVEL_FATAL   = 0,
    LOG_LEVEL_ERROR   = 1,
    LOG_LEVEL_WARNING = 2,
    LOG_LEVEL_NOTICE  = 3,
    LOG_LEVEL_INFO    = 3,
    LOG_LEVEL_TRACE   = 4,
    LOG_LEVEL_DEBUG   = 5,
};

namespace internal {
LogLevel get_log_level();
void set_log_level(LogLevel level);
void log_handler(LogLevel level, const char* filename, int line, const char *fmt, ...);
} // namespace internal

#define SOFA_PBRPC_SET_LOG_LEVEL(level) \
    ::sofa::pbrpc::internal::set_log_level(::sofa::pbrpc::LOG_LEVEL_##level)

#define SLOG(level, fmt, arg...) \
    (::sofa::pbrpc::LOG_LEVEL_##level > ::sofa::pbrpc::internal::get_log_level()) ? \
            (void)0 : ::sofa::pbrpc::internal::log_handler( \
                    ::sofa::pbrpc::LOG_LEVEL_##level, __FILE__, __LINE__, fmt, ##arg) \

#define SLOG_IF(condition, level, fmt, arg...) \
    !(condition) ? (void)0 : ::sofa::pbrpc::internal::log_handler( \
            ::sofa::pbrpc::LOG_LEVEL_##level, __FILE__, __LINE__, fmt, ##arg)

#if defined( LOG )
#define SCHECK(expression) CHECK(expression)
#define SCHECK_EQ(a, b) CHECK_EQ(a, b)
#define SCHECK_NE(a, b) CHECK_NE(a, b)
#define SCHECK_LT(a, b) CHECK_LT(a, b)
#define SCHECK_LE(a, b) CHECK_LE(a, b)
#define SCHECK_GT(a, b) CHECK_GT(a, b)
#define SCHECK_GE(a, b) CHECK_GE(a, b)
#else
#define SCHECK(expression) \
    SLOG_IF(!(expression), FATAL, "CHECK failed: " #expression)
#define SCHECK_EQ(a, b) SCHECK((a) == (b))
#define SCHECK_NE(a, b) SCHECK((a) != (b))
#define SCHECK_LT(a, b) SCHECK((a) <  (b))
#define SCHECK_LE(a, b) SCHECK((a) <= (b))
#define SCHECK_GT(a, b) SCHECK((a) >  (b))
#define SCHECK_GE(a, b) SCHECK((a) >= (b))
#endif

} // namespace pbrpc
} // namespace sofa

#endif // _SOFA_PBRPC_COMMON_H_

/* vim: set ts=4 sw=4 sts=4 tw=100 */
