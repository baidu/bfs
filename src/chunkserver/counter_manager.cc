// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#include "chunkserver/counter_manager.h"

#include <common/counter.h>
#include <common/timer.h>

namespace baidu {
namespace bfs {

common::Counter g_block_buffers;
common::Counter g_buffers_new;
common::Counter g_buffers_delete;
common::Counter g_blocks;
common::Counter g_writing_blocks;
common::Counter g_pending_writes;
common::Counter g_unfinished_bytes;
common::Counter g_writing_bytes;
common::Counter g_find_ops;
common::Counter g_read_ops;
common::Counter g_read_bytes;
common::Counter g_write_ops;
common::Counter g_write_bytes;
common::Counter g_recover_bytes;
common::Counter g_recover_count;
common::Counter g_refuse_ops;
common::Counter g_rpc_delay;
common::Counter g_rpc_delay_all;
common::Counter g_rpc_count;
common::Counter g_data_size;


CounterManager::CounterManager() {
    last_gather_time_ = common::timer::get_micros();
    memset(&counters_, 0 ,sizeof(counters_));
}

void CounterManager::GatherCounters() {
    int64_t now = common::timer::get_micros();
    int64_t interval = now - last_gather_time_;
    last_gather_time_ = now;

    Counters counters;
    counters.rpc_count = g_rpc_count.Clear();
    counters.rpc_delay = 0;
    counters.delay_all = 0;
    if (counters.rpc_count) {
        counters.rpc_delay = g_rpc_delay.Clear() / counters.rpc_count / 1000;
        counters.delay_all = g_rpc_delay_all.Clear() / counters.rpc_count / 1000;
    }
    counters.find_ops = g_find_ops.Clear() * 1000000 / interval;
    counters.read_ops = g_read_ops.Clear() * 1000000 / interval;
    counters.read_bytes = g_read_bytes.Clear() * 1000000 / interval;
    counters.write_ops = g_write_ops.Clear() * 1000000 / interval;
    counters.refuse_ops = g_refuse_ops.Clear() * 1000000 / interval;
    counters.write_bytes = g_write_bytes.Clear() * 1000000 / interval;
    counters.recover_bytes = g_recover_bytes.Clear() * 1000000 / interval;
    counters.buffers_new = g_buffers_new.Clear() * 1000000 / interval;
    counters.buffers_delete = g_buffers_delete.Clear() * 1000000 / interval;
    counters.unfinished_write_bytes = g_unfinished_bytes.Get();
    MutexLock lock(&counters_lock_);
    counters_ = counters;
}

CounterManager::Counters CounterManager::GetCounters() {
    MutexLock lock(&counters_lock_);
    return counters_;
}

} // namespace bfs
} // namespace baidu

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
