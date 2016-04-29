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

MasterSlave::MasterSlave() : scan_log_(0) {
    log_ = open("sync.log", O_WRONLY | O_CREAT | O_TRUNC, S_IRUSR);
    assert(log_ != -1);
}

bool MasterSlave::GetLeader(std::string* leader_addr) {
    return true;
}

bool MasterSlave::Log(int64_t seq, int32_t type, const std::string& key,
                      const std::string& value, bool is_last) {
    uint32_t encode_len = 4 + 8 + 4 + 4 + key.length() + 4 + value.length();
    char* entry = new char[encode_len];
    EncodeLog(seq, type, key, value, encode_len, entry);
    if (is_last) {
        std::pair<std::multimap<int64_t, std::string>::iterator,
                  std::multimap<int64_t, std::string>::iterator> ret = entries_.equal_range(seq);
        for (std::multimap<int64_t, std::string>::iterator it = ret.first; it != ret.second; ++it) {
            int w = write(log_, it->second.c_str(), it->second.length());
            if (w < 0) {
                LOG(WARNING, "[Sync] IOError logging");
                assert(0);
            }
        }
        int w = write(log_, entry, encode_len);
        if (w < 0) {
            LOG(WARNING, "[Sync] IOError logging %s", strerror(errno));
            assert(0);
        }
    } else {
        LOG(INFO, "[Sync]: sync log insert to map");
        entries_.insert(std::make_pair(seq, std::string(entry)));
    }
    delete[] entry;
    return true;
}

int MasterSlave::ScanLog() {
    scan_log_ = open("sync.log", O_RDONLY, S_IRUSR);
    if (errno == ENOENT) {
        return 0;
    }
    assert(scan_log_ != -1);
    return scan_log_;
}

int MasterSlave::Next(int64_t* seq, int32_t* type, char* key, char* value) {
    if (scan_log_ <= 0) {
        return -1;
    }
    char buf[4];
    uint32_t ret = read(scan_log_, buf, 4);
    assert(ret >= 0);
    if (ret == 0) {
        close(scan_log_);
        scan_log_ = 0;
        return 0;
    }
    uint32_t len;
    memcpy(&len, buf, 4);
    char* entry = new char(len);
    ret = read(scan_log_, entry, len);
    assert(ret == len);
    DecodeLog(entry, seq, type, key, value);
    return len;
}


void MasterSlave::EncodeLog(int64_t seq, int32_t type, const std::string& key,
                            const std::string& value, uint32_t encode_len, char* entry) {
    uint32_t key_len = key.length();
    uint32_t value_len = value.length();

    char* p = entry;
    memcpy(p, &encode_len, sizeof(encode_len));
    p += 4;
    memcpy(p, &seq, sizeof(seq));
    p += 8;
    memcpy(p, &type, sizeof(type));
    p += 4;
    memcpy(p, &key_len, sizeof(key_len));
    p += 4;
    memcpy(p, key.c_str(), key_len);
    p += key_len;
    memcpy(p, &value_len, sizeof(value_len));
    p += 4;
    memcpy(p, value.c_str(), value_len);
}

void MasterSlave::DecodeLog(char* const input, int64_t* seq, int32_t* type, char* key, char* value) {
    char* p = input;
    memcpy(seq, input, sizeof(*seq));
    p += sizeof(*seq);
    memcpy(type, p, sizeof(*type));
    p += sizeof(*type);
    uint32_t key_len, value_len;
    memcpy(&key_len, p, sizeof(key_len));
    p += sizeof(key_len);
    memcpy(key, p, key_len);
    p += key_len;
    memcpy(&value_len, p, sizeof(value_len));
    p += sizeof(value_len);
    memcpy(value, p, value_len);
}

} // namespace bfs
} // namespace baidu
