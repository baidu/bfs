// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#include "location_provider.h"

#include <stdio.h>

namespace baidu {
namespace bfs {

LocationProvider::LocationProvider(const std::string& hostname, const std::string& ipv4_address) {
    int a = 0;
    int b = 0;
    int c = 0;
    int d = 0;
    int ret = sscanf(&ipv4_address[0], "%d.%d.%d.%d", &a, &b, &c, &d);
    if (ret == 4) {
        char buf[12];
        snprintf(buf, 12, "%u_%u_%u", a, b, c);
        rack_ = buf;
    } else {
        rack_ = "default";
    }
    size_t pos = hostname.find('-');
    if (pos != std::string::npos) {
        datacenter_ = hostname.substr(0, pos);
        pos = datacenter_.find('0');
        if (pos != std::string::npos) {
            zone_ = datacenter_.substr(0, pos);
        } else {
            zone_ = datacenter_;
        }
        if (rack_ == "default") {
            rack_ = datacenter_ + "-default";
        }
    } else {
        zone_ = datacenter_ = "default";
    }
}

std::string LocationProvider::GetRack() const {
    return rack_;
}

std::string LocationProvider::GetDataCenter() const {
    return datacenter_;
}

std::string LocationProvider::GetZone() const {
    return zone_;
}

}
}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
