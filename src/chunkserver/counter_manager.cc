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
    memset(&stat_, 0 ,sizeof(stat_));
}

void ChunkserverCounterManager::GatherCounters(ChunkserverCounters* counters) {
    MutexLock lock(&mu_);
    int64_t now = common::timer::get_micros();
    int64_t interval = now - last_gather_time_;
    last_gather_time_ = now;

    stat_.rpc_count = counters->rpc_count.Clear();
    stat_.rpc_delay = 0;
    stat_.rpc_delay_all = 0;
    if (counters->rpc_count.Get()) {
        stat_.rpc_delay = counters->rpc_delay.Clear() / stat_.rpc_count / 1000;
        stat_.rpc_delay_all = counters->rpc_delay_all.Clear() / stat_.rpc_count / 1000;
    }
    stat_.find_ops = counters->find_ops.Clear() * 1000000 / interval;
    stat_.read_ops = counters->read_ops.Clear() * 1000000 / interval;
    stat_.read_bytes = counters->read_bytes.Clear() * 1000000 / interval;
    stat_.write_ops = counters->write_ops.Clear() * 1000000 / interval;
    stat_.refuse_ops = counters->refuse_ops.Clear() * 1000000 / interval;
    stat_.recover_bytes = counters->recover_bytes.Clear() * 1000000 / interval;
    stat_.unfinished_bytes = counters->unfinished_bytes.Get();
    stat_.recover_count = counters->recover_count.Get();
}

ChunkserverCounterManager::ChunkserverStat ChunkserverCounterManager::GetCounters() {
    MutexLock lock(&mu_);
    return stat_;
}

DiskCounterManager::DiskCounterManager() {
    last_gather_time_ = common::timer::get_micros();
    memset(&stat_, 0 ,sizeof(stat_));
}

void DiskCounterManager::GatherCounters(DiskCounterManager::DiskCounters* counters) {
    MutexLock lock(&mu_);
    int64_t now = common::timer::get_micros();
    int64_t interval = now - last_gather_time_;
    last_gather_time_ = now;

    stat_.buffers_new = counters->buffers_new.Clear() * 1000000 / interval;
    stat_.buffers_delete = counters->buffers_delete.Clear() * 1000000 / interval;
    stat_.write_bytes = counters->write_bytes.Clear() * 1000000 / interval;
    stat_.block_buffers = counters->block_buffers.Get();
    stat_.blocks = counters->blocks.Get();
    stat_.writing_blocks = counters->writing_blocks.Get();
    stat_.writing_bytes = counters->writing_bytes.Get();
    stat_.data_size = counters->data_size.Get();
    stat_.pending_writes = counters->pending_writes.Get();
}

DiskCounterManager::DiskStat DiskCounterManager::GetStat() {
    MutexLock lock(&mu_);
    return stat_;
}

} // namespace bfs
} // namespace baidu

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
