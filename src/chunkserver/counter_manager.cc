// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#include "chunkserver/counter_manager.h"

#include <common/timer.h>

namespace baidu {
namespace bfs {

ChunkserverCounterManager::ChunkserverCounterManager() {
    last_gather_time_ = common::timer::get_micros();
    memset(&stat_, 0, sizeof(stat_));
}

void ChunkserverCounterManager::GatherCounters(ChunkserverCounters* counters) {
    int64_t now = common::timer::get_micros();
    int64_t interval = now - last_gather_time_;
    last_gather_time_ = now;

    ChunkserverCounterManager::ChunkserverStat s;
    s.rpc_count = counters->rpc_count.Clear();
    s.rpc_delay = 0;
    s.rpc_delay_all = 0;
    if (counters->rpc_count.Get()) {
        s.rpc_delay = counters->rpc_delay.Clear() / s.rpc_count / 1000;
        s.rpc_delay_all = counters->rpc_delay_all.Clear() / s.rpc_count / 1000;
    }
    s.find_ops = counters->find_ops.Clear() * 1000000 / interval;
    s.read_ops = counters->read_ops.Clear() * 1000000 / interval;
    s.read_bytes = counters->read_bytes.Clear() * 1000000 / interval;
    s.write_ops = counters->write_ops.Clear() * 1000000 / interval;
    s.refuse_ops = counters->refuse_ops.Clear() * 1000000 / interval;
    s.recover_bytes = counters->recover_bytes.Clear() * 1000000 / interval;
    s.unfinished_bytes = counters->unfinished_bytes.Get();
    s.recover_count = counters->recover_count.Get();
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
    s.buffers_new = counters->buffers_new.Clear() * 1000000 / interval;
    s.buffers_delete = counters->buffers_delete.Clear() * 1000000 / interval;
    s.write_bytes = counters->write_bytes.Clear() * 1000000 / interval;
    s.block_buffers = counters->block_buffers.Get();
    s.blocks = counters->blocks.Get();
    s.writing_blocks = counters->writing_blocks.Get();
    s.writing_bytes = counters->writing_bytes.Get();
    s.data_size = counters->data_size.Get();
    s.pending_buf = counters->pending_buf.Get();
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
