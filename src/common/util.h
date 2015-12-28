// Copyright (c) 2014, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#ifndef  COMMON_UTIL_H_
#define  COMMON_UTIL_H_

#include <unistd.h>

#include <string>
#include <vector>

namespace common {
namespace util {

static const uint32_t kMaxHostNameSize = 255;
static inline std::string GetLocalHostName() {
    char str[kMaxHostNameSize + 1];
    if (0 != gethostname(str, kMaxHostNameSize + 1)) {
        return "";
    }
    std::string hostname(str);
    return hostname;
}

static const uint32_t MAX_PATH_LENGHT = 10240;
static inline bool SplitPath(const std::string& path,
               std::vector<std::string>* element,
               bool* isdir = NULL) {
    if (path.empty() || path[0] != '/' || path.size() > MAX_PATH_LENGHT) {
        return false;
    }
    element->clear();
    size_t last_pos = 0;
    for (size_t i = 1; i <= path.size(); i++) {
        if (path[i] == '/' || i == path.size()) {
            if (last_pos + 1 < i) {
                element->push_back(path.substr(last_pos + 1, i - last_pos - 1));
            }
            last_pos = i;
        }
    }
    if (isdir) {
        *isdir = (path[path.size() - 1] == '/');
    }
    return true;
}

static inline void EncodeBigEndian(char* buf, uint64_t value) {
    buf[0] = (value >> 56) & 0xff;
    buf[1] = (value >> 48) & 0xff;
    buf[2] = (value >> 40) & 0xff;
    buf[3] = (value >> 32) & 0xff;
    buf[4] = (value >> 24) & 0xff;
    buf[5] = (value >> 16) & 0xff;
    buf[6] = (value >> 8) & 0xff;
    buf[7] = value & 0xff;
}

} // namespace util
} // namespace common

#endif  //COMMON_UTIL_H_

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
