// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#ifndef  COMMON_TEST_UTILS_H_
#define  COMMON_TEST_UTILS_H_

#define ASSERT_TRUE(x) \
    do { \
        bool ret = x; \
        if (!ret) { \
            printf("\033[31m[Test failed]\033[0m: %d, %s\n", __LINE__, #x); \
            exit(1); \
        } else { \
            printf("\033[32m[Test pass]\033[0m: %s\n", #x); \
        } \
    } while(0)

#endif  //COMMON_TEST_UTILS_H_

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
