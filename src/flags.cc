// Copyright (c) 2014, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#include <gflags/gflags.h>

// nameserver
DEFINE_string(namedb_path, "./db", "Namespace database");
DEFINE_int64(namedb_cache_size, 1024L, "Namespace datebase memery cache size");
DEFINE_string(nameserver, "127.0.0.1", "Nameserver host");
DEFINE_string(nameserver_port, "8828", "Nameserver port");
DEFINE_int32(keepalive_timeout, 60, "Chunkserver keepalive timeout");
DEFINE_int32(default_replica_num, 3, "Default replica num of data block");
DEFINE_int32(nameserver_log_level, 2, "Nameserver log level");

// chunkserver
DEFINE_string(block_store_path, "./data", "Data path");
DEFINE_string(chunkserver_port, "8825", "Chunkserver port");
DEFINE_int32(heartbeat_interval, 5, "Heartbeat interval");
DEFINE_int32(blockreport_interval, 60, "blockreport_interval");
DEFINE_int32(chunkserver_log_level, 4, "Nameserver log level");
DEFINE_string(chunkserver_warninglog, "./wflog", "Warning log file");
DEFINE_int32(write_buf_size, 1024*1024, "Block write buffer size, bytes");


/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
