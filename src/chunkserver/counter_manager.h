// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#ifndef  BFS_COUNTER_MANAGER_H_
#define  BFS_COUNTER_MANAGER_H_

#include <stdint.h>
#include <string.h>
#include <string>

#include <common/mutex.h>
#include <common/counter.h>
#include <common/string_util.h>

namespace baidu {
namespace bfs {

class ChunkserverCounterManager {
public:
    struct ChunkserverCounters {
        common::Counter unfinished_bytes;
        common::Counter find_ops;
        common::Counter read_ops;
        common::Counter read_bytes;
        common::Counter write_ops;
        common::Counter recover_count;
        common::Counter recover_bytes;
        common::Counter refuse_ops;
        common::Counter rpc_delay;
        common::Counter rpc_delay_all;
        common::Counter rpc_count;
    };
    struct ChunkserverStat {
        int64_t unfinished_bytes;
        int64_t find_ops;
        int64_t read_ops;
        int64_t read_bytes;
        int64_t write_ops;
        int64_t recover_count;
        int64_t recover_bytes;
        int64_t refuse_ops;
        int64_t rpc_delay;
        int64_t rpc_delay_all;
        int64_t rpc_count;
        ChunkserverStat() :
            unfinished_bytes(0),
            find_ops(0),
            read_ops(0),
            read_bytes(0),
            write_ops(0),
            recover_count(0),
            recover_bytes(0),
            refuse_ops(0),
            rpc_delay(0),
            rpc_delay_all(0),
            rpc_count(0) {}
    };
public:
    ChunkserverCounterManager();
    void GatherCounters(ChunkserverCounters* counters);
    ChunkserverStat GetCounters();
private:
    Mutex mu_;
    ChunkserverStat stat_;
    int64_t last_gather_time_;
};

class DiskCounterManager {
public:
    struct DiskCounters {
        // number of buf, including the ones that are not big enough to push into block_buf_list_
        common::Counter block_buffers;
        // block number
        common::Counter blocks;
        // size of buf written a period of time (stat)
        common::Counter write_bytes;
        // number of blocks are being writing
        common::Counter writing_blocks;
        // size of buf writing to blocks
        // Inc when a buf is created (in Block::Write)
        // Dec when the buf is popped out by sliding window
        common::Counter writing_bytes;
        // data size
        common::Counter data_size;
        // number of buffers are created in a period of time (stat)
        common::Counter buffers_new;
        // number of buffers are deleted in a period of time (stat)
        common::Counter buffers_delete;
        // number of buf in waiting list (block_buf_list_), equivalent to block_buf_list_.size()
        // Inc when adding a buf to block_buf_list_
        // Dec when a buf is erased from block_buf_list_
        common::Counter pending_buf;
    };
    struct DiskStat {
        int64_t block_buffers;
        int64_t blocks;
        int64_t write_bytes;
        int64_t writing_blocks;
        int64_t writing_bytes;
        int64_t data_size;
        int64_t buffers_new;
        int64_t buffers_delete;
        int64_t pending_buf;
        double load;
        DiskStat() :
            block_buffers(0),
            blocks(0),
            write_bytes(0),
            writing_blocks(0),
            writing_bytes(0),
            data_size(0),
            buffers_new(0),
            buffers_delete(0),
            pending_buf(0),
            load(0.0) {}
        void ToString(std::string* str) {
            str->append("block_buf=" + common::HumanReadableString(block_buffers));
            str->append(" blocks=" + common::HumanReadableString(blocks));
            str->append(" w_bytes=" + common::HumanReadableString(write_bytes));
            str->append(" wing_blocks=" + common::HumanReadableString(writing_blocks));
            str->append(" wing_bytes=" + common::HumanReadableString(writing_bytes));
            str->append(" size=" + common::HumanReadableString(data_size));
            str->append(" new=" + common::HumanReadableString(buffers_new));
            str->append(" delete=" + common::HumanReadableString(buffers_delete));
            str->append(" pending_w=" + common::HumanReadableString(pending_buf));
        }
        bool operator<(const DiskStat& s) const {
            return s.load < load;
        }

    };
public:
    DiskCounterManager();
    void GatherCounters(DiskCounters* counters);
    DiskStat GetStat();
private:
    Mutex mu_;
    DiskStat stat_;
    int64_t last_gather_time_;
};

} // namespace bfs
} // namespace baidu

#endif  // BFS_COUNTER_MANAGER_H_

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
