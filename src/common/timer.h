// Copyright (c) 2014, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#ifndef  COMMON_TIMER_H_
#define  COMMON_TIMER_H_

#include <sys/time.h>

namespace common {

namespace timer {
static inline long get_micros() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return static_cast<long>(tv.tv_sec) * 1000000 + tv.tv_usec;
}

}
}

#endif  // COMMON_TIMER_H_

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
