// Copyright (c) 2014, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#ifndef  COMMON_LOGGING_H_
#define  COMMON_LOGGING_H_

#include "logging.h"

#include <unistd.h>
#include <syscall.h>
#include <sys/time.h>
#include <time.h>

#include <assert.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>

namespace common {

int g_log_level = 4;

void SetLogLevel(int level) {
    g_log_level = level;
}

void Logv(const char* format, va_list ap) {
    const uint64_t thread_id = syscall(__NR_gettid);

    // We try twice: the first time with a fixed-size stack allocated buffer,
    // and the second time with a much larger dynamically allocated buffer.
    char buffer[500];
    for (int iter = 0; iter < 2; iter++) {
        char* base;
        int bufsize;
        if (iter == 0) {
            bufsize = sizeof(buffer);
            base = buffer;
        } else {
            bufsize = 30000;
            base = new char[bufsize];
        }
        char* p = base;
        char* limit = base + bufsize;

        struct timeval now_tv;
        gettimeofday(&now_tv, NULL);
        const time_t seconds = now_tv.tv_sec;
        struct tm t;
        localtime_r(&seconds, &t);
        p += snprintf(p, limit - p,
                "%02d/%02d %02d:%02d:%02d.%06d %lld ",
                t.tm_mon + 1,
                t.tm_mday,
                t.tm_hour,
                t.tm_min,
                t.tm_sec,
                static_cast<int>(now_tv.tv_usec),
                static_cast<long long unsigned int>(thread_id));

        // Print the message
        if (p < limit) {
            va_list backup_ap;
            va_copy(backup_ap, ap);
            p += vsnprintf(p, limit - p, format, backup_ap);
            va_end(backup_ap);
        }

        // Truncate to available space if necessary
        if (p >= limit) {
            if (iter == 0) {
                continue;       // Try again with larger buffer
            } else {
                p = limit - 1;
            }
        }

        // Add newline if necessary
        if (p == base || p[-1] != '\n') {
            *p++ = '\n';
        }

        assert(p <= limit);
        fwrite(base, 1, p - base, stdout);
        fflush(stdout);
        if (base != buffer) {
            delete[] base;
        }
        break;
    }
}

void Log(int level, const char* fmt, ...) {
    va_list ap;
    va_start(ap, fmt);

    if (level >= g_log_level) {
        Logv(fmt, ap);
    }
    va_end(ap);
}

} // namespace common

#endif  // COMMON_LOGGING_H_

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
