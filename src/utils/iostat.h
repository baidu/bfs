// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.


#ifndef BFS_IOSTAT_H_
#define BFS_IOSTAT_H_

#include <stdint.h>
#include <string>

namespace baidu {
namespace bfs {
    struct IOStat {
        unsigned long read_ios;
        unsigned long read_merges;
        unsigned long long read_sectors;
        unsigned long read_ticks;
        unsigned long write_ios;
        unsigned long write_merges;
        unsigned long long write_sectors;
        unsigned long write_ticks;
        unsigned long  in_flight;
        unsigned long io_ticks;
        unsigned long time_in_queue;
    };

    bool GetIOStat(const std::string& path, struct IOStat *iostat);

} //namespace bfs
} // namespace baidu

#endif
