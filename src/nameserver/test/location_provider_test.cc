// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#include "nameserver/location_provider.h"

#include <gtest/gtest.h>

namespace baidu {
namespace bfs {

class LocationProviderTest : public ::testing::Test {
};

TEST_F(LocationProviderTest, LocationProviderTest) {
    LocationProvider loc1("nmg03-bfs-master0-", "10.82.199.32");
    ASSERT_EQ(loc1.GetRack(), std::string("10_82_199"));
    ASSERT_EQ(loc1.GetDataCenter(), std::string("nmg03"));
    ASSERT_EQ(loc1.GetZone(), std::string("nmg"));
    LocationProvider loc2("yq02-bfs-master0-", "192.168.0.1");
    ASSERT_EQ(loc2.GetRack(), std::string("192_168_0"));
    ASSERT_EQ(loc2.GetDataCenter(), std::string("yq02"));
    ASSERT_EQ(loc2.GetZone(), std::string("yq"));
    LocationProvider loc3("bj-bfs-master-", "192.168.0.1");
    ASSERT_EQ(loc3.GetDataCenter(), std::string("bj"));
    ASSERT_EQ(loc3.GetZone(), std::string("bj"));

    LocationProvider loc4("hk01-xxoo", "19216801");
    ASSERT_EQ(loc4.GetRack(), std::string("hk01-default"));

    LocationProvider loc5("xxoo", "19216801");
    ASSERT_EQ(loc5.GetRack(), std::string("default"));
    ASSERT_EQ(loc5.GetDataCenter(), std::string("default"));
    ASSERT_EQ(loc5.GetZone(), std::string("default"));
}
}
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
