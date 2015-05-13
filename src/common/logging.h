// Copyright (c) 2014, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#ifndef  COMMON_LOGGING_H_
#define  COMMON_LOGGING_H_

namespace common {

enum LogLevel {
    DEBUG = 2,
    INFO = 4,
    WARNING = 8,
    FATAL = 16,
};

void SetLogLevel(int level);

void Log(int level, const char* fmt, ...);

}

#define LOG(level, fmt, args...) Log(level, "[%s:%d] "fmt, __FILE__, __LINE__, ##args)

using common::DEBUG;
using common::INFO;
using common::WARNING;
using common::FATAL;

#endif  // COMMON_LOGGING_H_

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
