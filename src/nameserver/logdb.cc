// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//

#include <errno.h>
#include <common/logging.h>
#include <boost/bind.hpp>
#include "nameserver/logdb.h"

namespace baidu {
namespace bfs {

LogDB::LogDB(const DBOption& option) : dbpath_(option.path),
                                       snapshot_interval_(option.snapshot_interval),
                                       write_log_(NULL), read_log_(NULL),
                                       write_index_(NULL), read_index_(NULL),
                                       marker_log_(NULL) {
    if(!RecoverMarker()) {
        LOG(WARNING, "[LogDB] RecoverMarker failed reason: %s", strerror(errno));
        assert(0);
    }
    thread_pool_ = new ThreadPool(10);
    WriteMarkerSnapshot();
}

LogDB::~LogDB() {
    thread_pool_->Stop(true);
    if (write_log_) fclose(write_log_);
    if (read_log_) fclose(read_log_);
    if (write_index_) fclose(write_index_);
    if (read_index_) fclose(read_index_);
    if (marker_log_) fclose(marker_log_);
}

StatusCode LogDB::Write(int64_t index, const std::string& entry) {

    return kOK;
}

StatusCode LogDB::Read(int64_t index, std::string* entry) {
    return kOK;
}

StatusCode LogDB::WriteMarker(const std::string& key, const std::string& value) {
    std::string data;
    EncodeMarker(MarkerEntry(key, value), &data);
    MutexLock lock(&mu_);
    if (fwrite(data.c_str(), 1, data.length(), marker_log_) != data.length()) {
        LOG(WARNING, "[LogDB] WriteMarker failed key = %s value = %s", key.c_str(), value.c_str());
        return kWriteError;
    }
    markers_[key] = value;
    return kOK;
}

StatusCode LogDB::WriteMarker(const std::string& key, int64_t value) {
    char buf[8];
    memcpy(buf, &value, 8);
    return WriteMarker(key, std::string(buf, 8));
}

StatusCode LogDB::ReadMarker(const std::string& key, std::string* value) {
    MutexLock lock(&mu_);
    std::map<std::string, std::string>::iterator it = markers_.find(key);
    if (it == markers_.end()) {
        return kNotFound;
    }
    *value = it->second;
    return kOK;
}

StatusCode LogDB::ReadMarker(const std::string& key, int64_t* value) {
    std::string v;
    StatusCode status = ReadMarker(key, &v);
    if (status != kOK) {
        return status;
    }
    memcpy(value, &(v[0]), 8);
    return kOK;
}

StatusCode LogDB::GetLargestIdx(int64_t* value) {
    return kOK;
}

void LogDB::DeleteUpTo(int64_t index) {

}

void LogDB::DeleteFrom(int64_t index) {

}

bool LogDB::RecoverMarker() {
    // recover markers
    FILE* fp = fopen("marker.mak", "r");
    if (fp == NULL) {
        if (errno == ENOENT) {
            fp = fopen("marker.tmp", "r");
        }
    }
    if (fp == NULL) {
        LOG(INFO, "[LogDB] No marker to recover");
        return errno == ENOENT;
    }
    std::string data;
    while (true) {
        int ret = ReadOne(fp, &data);
        if (ret == 0)  break;
        if (ret < 0) {
            LOG(WARNING, "[LogDB] RecoverMarker failed while reading");
            return false;
        }
        MarkerEntry mark;
        DecodeMarker(data, &mark);
        markers_[mark.key] = mark.value;
    }
    fclose(fp);
    LOG(INFO, "[LogDB] Recover markers done");

    rename("marker.tmp", "marker.mak");
    return true;
}

void LogDB::WriteMarkerSnapshot() {
    MutexLock lock(&mu_);
    if (marker_log_) fclose(marker_log_);
    FILE* fp = fopen("marker.tmp", "w");
    if (fp == NULL) {
        LOG(WARNING, "[LogDB] open marker.tmp failed %s", strerror(errno));
        return;
    }
    std::string data;
    for (std::map<std::string, std::string>::iterator it = markers_.begin();
            it != markers_.end(); ++it) {
        MarkerEntry marker(it->first, it->second);
        EncodeMarker(marker, &data);
        if (fwrite(data.c_str(), 1, data.length(), fp) != data.length()) {
            LOG(WARNING, "[LogDB] write marker.tmp failed %s", strerror(errno));
            return;
        }
    }
    fclose(fp);
    rename("marker.tmp", "marker.mak");
    marker_log_ = fopen("marker.mak", "a");
    if (marker_log_ == NULL) {
        LOG(WARNING, "[LogDB] open marker.mak failed %s", strerror(errno));
        return;
    }
    LOG(INFO, "[LogDB] WriteMarkerSnapshot done");
    thread_pool_->DelayTask(snapshot_interval_, boost::bind(&LogDB::WriteMarkerSnapshot, this));
}

int LogDB::ReadOne(FILE* fp, std::string* data) {
    int len;
    int ret = fread(&len, 1, 4, fp);
    if (ret == 0) {
        return 0;
    }
    if (ret != 4) return -1;
    char* buf = new char[len];
    ret = fread(buf, 1, len, fp);
    if (ret != len) return -1;
    data->clear();
    data->assign(buf, len);
    delete[] buf;
    return len;
}

void LogDB::EncodeLogEntry(const LogDataEntry& log, std::string* data) {
    int len = 8 + (log.entry).length(); // index + entry
    char buf[12];
    memcpy(buf, &len, 4);
    memcpy(buf + 4, &(log.index), 8);
    data->clear();
    data->append(buf, 12);
    data->append(log.entry);
}

void LogDB::DecodeLogEntry(const std::string& data, LogDataEntry* log) { // data = index + log_entry
    memcpy(&(log->index), &(data[0]), 8);
    (log->entry).assign(data.substr(8));
}

void LogDB::EncodeMarker(const MarkerEntry& marker, std::string* data) {
    int klen = (marker.key).length();
    int vlen = (marker.value).length();
    int len = 4 + klen + 4 + vlen; // klen + key + vlen + value
    char buf[4];
    memcpy(buf, &len, 4);
    data->clear();
    data->append(buf, 4);
    memcpy(buf, &klen, 4);
    data->append(buf, 4);
    data->append(marker.key);
    memcpy(buf, &vlen, 4);
    data->append(buf, 4);
    data->append(marker.value);
}

void LogDB::DecodeMarker(const std::string& data, MarkerEntry* marker) { // data = klen + k + vlen + v
    int klen;
    memcpy(&klen, &(data[0]), 4);
    (marker->key).assign(data.substr(4, klen));
    int vlen;
    memcpy(&vlen, &(data[4 + klen]), 4);
    (marker->value).assign(data.substr(4 + klen + 4, vlen));
}


} // namespace bfs
} // namespace baidu
