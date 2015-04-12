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
std::string GetLocalHostName() {
    char str[kMaxHostNameSize + 1];
    if (0 != gethostname(str, kMaxHostNameSize + 1)) {
        return "";
    }
    std::string hostname(str);
    return hostname;
}


static const uint32_t MAX_PATH_LENGHT = 10240;
static const uint32_t MAX_PATH_DEPTH = 99;
bool SplitPath(const std::string& path, std::vector<std::string>* element,
               bool* isdir) {
    if (path.empty() || path[0] != '/' || path.size() > MAX_PATH_LENGHT) {
        return false;
    }
    size_t last_pos = 0;
    for (size_t i = 1; i <= path.size(); i++) {
        if (path[i] == '/' || i == path.size()) {
            if (last_pos + 1 < i) {
                element->push_back(path.substr(last_pos + 1, i - last_pos - 1));
            }
            last_pos = i;
        }
    }
    *isdir = (path[path.size() - 1] == '/');
    return true;
}

}
}

#endif  //COMMON_UTIL_H_

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
