// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#include "chunkserver/counter_manager.h"

#include <common/timer.h>

namespace baidu {
namespace bfs {

// number of buf, including the ones that are not big enough to push into block_buf_list_
common::Counter g_block_buffers;
// number of buffers are created in a period of time (stat)
common::Counter g_buffers_new;
// number of buffers are deleted in a period of time (stat)
common::Counter g_buffers_delete;
common::Counter g_unfinished_bytes;
common::Counter g_find_ops;
common::Counter g_read_ops;
common::Counter g_read_bytes;
common::Counter g_write_ops;
common::Counter g_recover_count;
common::Counter g_recover_bytes;
common::Counter g_refuse_ops;
common::Counter g_rpc_delay;
common::Counter g_rpc_delay_all;
common::Counter g_rpc_count;

ChunkserverCounterManager::ChunkserverCounterManager() {
    last_gather_time_ = common::timer::get_micros();
    memset(&stat_, 0, sizeof(stat_));
}

void ChunkserverCounterManager::GatherCounters() {
    int64_t now = common::timer::get_micros();
    int64_t interval = now - last_gather_time_;
    last_gather_time_ = now;

    ChunkserverCounterManager::ChunkserverStat s;
    s.rpc_count = g_rpc_count.Clear();
    s.rpc_delay = 0;
    s.rpc_delay_all = 0;
    if (g_rpc_count.Get()) {
        s.rpc_delay = g_rpc_delay.Clear() / s.rpc_count / 1000;
        s.rpc_delay_all = g_rpc_delay_all.Clear() / s.rpc_count / 1000;
    }
    s.buffers_new = g_buffers_new.Clear() * 1000000 / interval;
    s.buffers_delete = g_buffers_delete.Clear() * 1000000 / interval;
    s.find_ops = g_find_ops.Clear() * 1000000 / interval;
    s.read_ops = g_read_ops.Clear() * 1000000 / interval;
    s.read_bytes = g_read_bytes.Clear() * 1000000 / interval;
    s.write_ops = g_write_ops.Clear() * 1000000 / interval;
    s.refuse_ops = g_refuse_ops.Clear() * 1000000 / interval;
    s.recover_bytes = g_recover_bytes.Clear() * 1000000 / interval;
    s.unfinished_bytes = g_unfinished_bytes.Get();
    s.recover_count = g_recover_count.Get();
    MutexLock lock(&mu_);
    stat_ = s;
}

ChunkserverCounterManager::ChunkserverStat ChunkserverCounterManager::GetCounters() {
    MutexLock lock(&mu_);
    return stat_;
}

DiskCounterManager::DiskCounterManager() {
    last_gather_time_ = common::timer::get_micros();
    memset(&stat_, 0, sizeof(stat_));
}

void DiskCounterManager::GatherCounters(DiskCounterManager::DiskCounters* counters) {
    int64_t now = common::timer::get_micros();
    int64_t interval = now - last_gather_time_;
    last_gather_time_ = now;

    DiskCounterManager::DiskStat s;
    s.buf_write_bytes = counters->buf_write_bytes.Clear() * 1000000 / interval;
    s.disk_write_bytes = counters->disk_write_bytes.Clear() * 1000000 / interval;
    s.blocks = counters->blocks.Get();
    s.writing_blocks = counters->writing_blocks.Get();
    s.writing_bytes = counters->writing_bytes.Get();
    s.data_size = counters->data_size.Get();
    s.pending_buf = counters->pending_buf.Get();
    s.mem_read_ops = counters->mem_read_ops.Clear() * 1000000  / interval;
    s.disk_read_ops = counters->disk_read_ops.Clear() * 1000000  / interval;

    MutexLock lock(&mu_);
    stat_ = s;
}

DiskCounterManager::DiskStat DiskCounterManager::GetStat() {
    MutexLock lock(&mu_);
    return stat_;
}

} // namespace bfs
} // namespace baidu

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
