// Copyright (c) 2014, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#include <gflags/gflags.h>

// global
DEFINE_bool(bfs_bug_tolerant, true, "Tolerate minor bug");
DEFINE_bool(bfs_web_kick_enable, false, "Enable kick button on website");
DEFINE_string(bfs_log, "", "BFS log");
DEFINE_int32(bfs_log_size, 1024, "BFS log size");
// nameserver
DEFINE_string(namedb_path, "./db", "Namespace database");
DEFINE_int64(namedb_cache_size, 1024L, "Namespace datebase memery cache size");
DEFINE_string(nameserver, "127.0.0.1", "Nameserver host");
DEFINE_string(nameserver_port, "8828", "Nameserver port");
DEFINE_int32(keepalive_timeout, 10, "Chunkserver keepalive timeout");
DEFINE_int32(default_replica_num, 3, "Default replica num of data block");
DEFINE_int32(nameserver_log_level, 4, "Nameserver log level");
DEFINE_string(nameserver_warninglog, "./wflog", "Warning log file");
DEFINE_int32(nameserver_safemode_time, 120, "Nameserver leave safemode time in ms");
DEFINE_int32(recover_speed, 100, "max num of block to recover for one chunkserver");
DEFINE_int32(recover_dest_limit, 5, "Number of recover dest");
DEFINE_int32(recover_timeout, 180, "Recover timeout for one chunkserver");
DEFINE_bool(clean_redundancy, false, "Clean redundant replica");
DEFINE_int32(nameserver_report_thread_num, 20, "Threads to handle block report");
DEFINE_int32(nameserver_work_thread_num, 20, "Work threads num");
DEFINE_bool(select_chunkserver_by_zone, false, "Select chunkserver by zone");
DEFINE_double(select_chunkserver_local_factor, 0.1, "Weighting factors of locality");
DEFINE_int32(blockmapping_bucket_num, 20, "Partation num of blockmapping");

// chunkserver
DEFINE_string(block_store_path, "./data", "Data path");
DEFINE_string(chunkserver_port, "8825", "Chunkserver port");
DEFINE_int32(heartbeat_interval, 1, "Heartbeat interval");
DEFINE_int32(blockreport_interval, 10, "blockreport_interval");
DEFINE_int32(blockreport_size, 2000, "blockreport_size");
DEFINE_int32(chunkserver_log_level, 4, "Chunkserver log level");
DEFINE_string(chunkserver_warninglog, "./wflog", "Warning log file");
DEFINE_int32(write_buf_size, 1024*1024, "Block write buffer size, bytes");
DEFINE_int32(chunkserver_max_pending_buffers, 10240, "Buffer num wait flush to disk");
DEFINE_int32(chunkserver_work_thread_num, 10, "Chunkserver work thread num");
DEFINE_int32(chunkserver_read_thread_num, 20, "Chunkserver work thread num");
DEFINE_int32(chunkserver_write_thread_num, 10, "Chunkserver work thread num");
DEFINE_int32(chunkserver_io_thread_num, 10, "Chunkserver io thread num");
DEFINE_int32(chunkserver_recover_thread_num, 10, "Chunkserver work thread num");
DEFINE_int32(chunkserver_file_cache_size, 1000, "Chunkserver file cache size");
DEFINE_int32(chunkserver_use_root_partition, 1, "Should chunkserver use root partition, 0: forbidden");
DEFINE_bool(chunkserver_auto_clean, true, "If namespace version mismatch, chunkserver clean itself");
// SDK
DEFINE_int32(sdk_thread_num, 10, "Sdk thread num");
DEFINE_int32(sdk_file_reada_len, 1024*1024, "Read ahead buffer len");
DEFINE_string(sdk_write_mode, "chains", "Sdk write mode: chains/fan-out");
DEFINE_int32(sdk_createblock_retry, 5, "Create block retry times before fail");

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
