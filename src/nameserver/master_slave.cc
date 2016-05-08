// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//

#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <common/string_util.h>
#include <common/logging.h>

#include "nameserver/sync.h"

namespace baidu {
namespace bfs {

MasterSlave::MasterSlave() : scan_log_(0) {}

void MasterSlave::Init() {
    log_ = open("sync.log", O_WRONLY | O_CREAT, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP);
    if (log_ < 0) {
        LOG(FATAL, "open sync log failed reason:%s", strerror(errno));
    }
}

bool MasterSlave::GetLeader(std::string* leader_addr) {
    return true;
}

bool MasterSlave::Log(const std::string& entry) {
    int w = write(log_, entry.c_str(), entry.length());
    return w >= 0;
}

int MasterSlave::ScanLog() {
    scan_log_ = open("sync.log", O_RDONLY);
    if (scan_log_ < 0 && errno == ENOENT) {
        LOG(INFO, "can't find sync log %d", scan_log_);
        return -1;
    }
    assert(scan_log_ != -1);
    return scan_log_;
}

int MasterSlave::Next(char* entry) {
    if (scan_log_ <= 0) {
        return -1;
    }
    char buf[4];
    uint32_t ret = read(scan_log_, buf, 4);
    assert(ret >= 0);
    if (ret == 0) {
        close(scan_log_);
        scan_log_ = 0;
        rename("sync.log", "sync.bak");
        return 0;
    } else if (ret < 4) {
        LOG(WARNING, "incomplete record");
        return ret;
    }
    uint32_t len;
    memcpy(&len, buf, 4);
    LOG(INFO, "[Sync] read_len=%u", len);
    ret = read(scan_log_, entry, len);
    if (ret < len) {
        LOG(WARNING, "incomplete record");
        return len;
    }
    assert(ret == len);
    return len;
}

} // namespace bfs
} // namespace baidu
