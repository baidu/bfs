// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#ifndef  BFS_LOCATION_PROVIDER_H_
#define  BFS_LOCATION_PROVIDER_H_

#include <stdint.h>
#include <string>

namespace baidu {
namespace bfs {

class LocationProvider {
public:
    LocationProvider(const std::string& hostname, const std::string& ipv4_address);
    std::string GetRack() const;
    std::string GetDataCenter() const;
    std::string GetZone() const;
private:
    std::string rack_;
    std::string datacenter_;
    std::string zone_;
};

} // namespace bfs
} // namespace bfs

#endif  //__LOCATION_PROVIDER_H_

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
