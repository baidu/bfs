// Copyright (c) 2014, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#include <stdint.h>
#include <string>

// nameserver
std::string FLAGS_namedb_path = "./db";
int64_t FLAGS_namedb_cache_size = 1024L;
std::string FLAGS_nameserver = "127.0.0.1:8028";
std::string FLAGS_nameserver_port = "8028";

// chunkserver
std::string FLAGS_block_store_path = "./data";
std::string FLAGS_chunkserver_port = "8825";
int32_t FLAGS_heartbeat_interval = 5;
int32_t FLAGS_blockreport_interval = 600;


/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
