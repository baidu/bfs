// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#define private public
#include "chunkserver/file_cache.h"

#include <gflags/gflags.h>
#include <gtest/gtest.h>

DECLARE_string(namedb_path);

namespace baidu {
namespace bfs {

class FileCacheTest : public ::testing::Test {
public:
    FileCacheTest() {}
protected:
};

TEST_F(FileCacheTest, ReadFile) {
    FileCache fcache(1024);
    char buf[1024];
    ASSERT_NE(0, fcache.ReadFile("/test", buf, 1024L, 0L));
    ASSERT_NE(0, fcache.ReadFile("/test", buf, 1024L, 1024L));
}

}
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
