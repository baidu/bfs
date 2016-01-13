// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#ifndef  BFS_COUNTER_MANAGER_H_
#define  BFS_COUNTER_MANAGER_H_

#include <stdint.h>
#include <string.h>

#include <common/mutex.h>

namespace baidu {
namespace bfs {

class CounterManager {
public:
    struct Counters {
        int64_t rpc_count;
        int64_t rpc_delay;
        int64_t delay_all;
        int64_t find_ops;
        int64_t read_ops;
        int64_t write_ops;
        int64_t refuse_ops;
        int64_t write_bytes;
        int64_t read_bytes;
        int64_t recover_bytes;
        int64_t file_cache_hits;
        int64_t file_cache_miss;
        int64_t buffers_new;
        int64_t buffers_delete;
    };
    CounterManager();
    void GatherCounters();
    Counters GetCounters();
private:
    Mutex counters_lock_;
    Counters counters_;
    int64_t last_gather_time_;
};

} // namespace bfs
} // namespace baidu

#endif  // BFS_COUNTER_MANAGER_H_

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
