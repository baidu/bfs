// Copyright (c) 2014, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#ifndef  COMMON_UTIL_H_
#define  COMMON_UTIL_H_

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

}
}

#endif  //COMMON_UTIL_H_

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
