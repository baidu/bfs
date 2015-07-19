// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#ifndef  COMMON_STRING_UTIL_H_
#define  COMMON_STRING_UTIL_H_

#include <stdint.h>
#include <stdio.h>
#include <string>
#include <vector>

namespace common {

static inline void SplitString(const std::string& full,
                               const std::string& delim,
                               std::vector<std::string>* result) {
    result->clear();
    if (full.empty()) {
        return;
    }

    std::string tmp;
    std::string::size_type pos_begin = full.find_first_not_of(delim);
    std::string::size_type comma_pos = 0;

    while (pos_begin != std::string::npos) {
        comma_pos = full.find(delim, pos_begin);
        if (comma_pos != std::string::npos) {
            tmp = full.substr(pos_begin, comma_pos - pos_begin);
            pos_begin = comma_pos + delim.length();
        } else {
            tmp = full.substr(pos_begin);
            pos_begin = comma_pos;
        }

        if (!tmp.empty()) {
            result->push_back(tmp);
            tmp.clear();
        }
    }
}

static inline std::string TrimString(const std::string& str, const std::string& trim) {
    std::string::size_type pos = str.find_first_not_of(trim);
    if (pos == std::string::npos) {
        return "";
    }
    std::string::size_type pos2 = str.find_last_not_of(trim);
    if (pos2 != std::string::npos) {
        return str.substr(pos, pos2 - pos + 1);
    }
    return str.substr(pos);
}


static inline std::string NumToString(int64_t num) {
    char buf[32];
    snprintf(buf, sizeof(buf), "%ld", num);
    return std::string(buf);
}

static inline std::string NumToString(int num) {
    return NumToString(static_cast<int64_t>(num));
}

static inline std::string NumToString(double num) {
    char buf[32];
    snprintf(buf, sizeof(buf), "%.3f", num);
    return std::string(buf);
}

static inline std::string HumanReadableString(int64_t num) {
    static const int max_shift = 7;
    static const char* const prefix[max_shift] = {" ", " K", " M", " G", " T", " E", " Z"};
    int shift = 0;
    double v = num;
    while ((num>>=10) > 0 && shift < max_shift) {
        v /= 1024;
        shift++;
    }
    return NumToString(v) + prefix[shift];
}

}

#endif  // COMMON_STRING_UTIL_H_

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
