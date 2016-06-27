// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//

#include <errno.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <dirent.h>
#include <common/logging.h>
#include <common/string_util.h>
#include <boost/bind.hpp>
#include <boost/lexical_cast.hpp>

#include "nameserver/logdb.h"

namespace baidu {
namespace bfs {

LogDB::LogDB() : largest_index_(-1), smallest_index_(-1), current_log_index_(-1),
                 write_log_(NULL), write_index_(NULL), marker_log_(NULL) {}

LogDB::~LogDB() {
    thread_pool_->Stop(true);
    if (write_log_) fclose(write_log_);
    for (FileCache::iterator it = read_log_.begin(); it != read_log_.end(); ++it) {
        fclose((it->second).first);
        fclose((it->second).second);
    }
    if (write_index_) fclose(write_index_);
    if (marker_log_) fclose(marker_log_);
}

void LogDB::Open(const std::string& path, const DBOption& option, LogDB** dbptr) {
    *dbptr = NULL;

    LogDB* logdb = new LogDB();
    logdb->dbpath_ = path + "/";
    logdb->snapshot_interval_ = option.snapshot_interval * 1000;
    logdb->log_size_ = option.log_size << 20;
    mkdir(logdb->dbpath_.c_str(), 0755);
    if(!logdb->RecoverMarker()) {
        LOG(WARNING, "[LogDB] RecoverMarker failed reason: %s", strerror(errno));
        delete logdb;
        return;
    }
    std::map<std::string, std::string>::iterator it = logdb->markers_.find(".smallest_index_");
    if (it != logdb->markers_.end()) {
        logdb->smallest_index_ = boost::lexical_cast<int64_t>(it->second);
    }
    if (!logdb->BuildFileCache()) {
        LOG(WARNING, "[LogDB] BuildFileCache failed reason: %s", strerror(errno));
        delete logdb;
        return;
    }
    logdb->thread_pool_ = new ThreadPool(10);
    logdb->WriteMarkerSnapshot();
    *dbptr = logdb;
    return;
}

StatusCode LogDB::Write(int64_t index, const std::string& entry) {
    MutexLock lock(&mu_);
    if (largest_index_ == -1) {
        WriteMarkerNoLock(".smallest_index_", common::NumToString(index));
        smallest_index_ = index;
    } else if (index != largest_index_ + 1) {
        LOG(INFO, "[LogDB] Write with invalid index = %ld largest_index_ = %ld ",
                    index, largest_index_);
        return kBadParameter;
    }
    std::string data;
    uint32_t len = 8 + entry.length();
    data.append(reinterpret_cast<char*>(&len), 4);
    EncodeLogEntry(LogDataEntry(index, entry), &data);
    if (!write_log_) {
        if (!NewWriteLog(index)) {
            return kWriteError;
        }
    }
    int64_t offset = ftell(write_log_);
    if (offset > log_size_) {
        fclose(write_log_);
        fclose(write_index_);
        if (!NewWriteLog(index)) {
            return kWriteError;
        }
        offset = 0;
    }
    StatusCode status = kOK;
    if (fwrite(data.c_str(), 1, data.length(), write_log_) != data.length() || fflush(write_log_) != 0) {
        LOG(WARNING, "[LogDB] Write log %ld failed", index);
        status =  kWriteError;
    }
    if (fwrite(reinterpret_cast<char*>(&index), 1, 8, write_index_) != 8) {
        LOG(WARNING, "[LogDB] Write index %ld failed", index);
        status = kWriteError;
    }
    if (fwrite(reinterpret_cast<char*>(&offset), 1, 8, write_index_) != 8 || fflush(write_index_) != 0) {
        LOG(WARNING, "[LogDB] Write index %ld failed", index);
        status = kWriteError;
    }
    if (status == kWriteError) {
        fclose(write_log_);
        fclose(write_index_);
        write_log_ = NULL;
        write_index_ = NULL;
        return kWriteError;
    }
    largest_index_ = index;
    return kOK;
}

StatusCode LogDB::Read(int64_t index, std::string* entry) {
    if (read_log_.empty() || index > largest_index_ || index < smallest_index_) {
        return kNotFound;
    }
    FileCache::iterator it = read_log_.lower_bound(index);
    if (it == read_log_.end() || (it != read_log_.begin() && index != it->first)) {
        --it;
    }
    if (index < it->first) {
        LOG(WARNING, "[LogDB] Read cannot find index file %ld ", index);
    }
    FILE* idx_fp = (it->second).first;
    FILE* log_fp = (it->second).second;
    // find entry offset
    int offset = 16 * (index - it->first);
    mu_.Lock();
    if (fseek(idx_fp, offset, SEEK_SET) != 0) {
        LOG(WARNING, "[LogDB] Read cannot find index file %ld ", index);
    }
    char buf[16];
    int ret = fread(buf, 1, 16, idx_fp);
    mu_.Unlock();
    if (ret == 0) {
        return kNotFound;
    } else if (ret != 16) {
        LOG(WARNING, "[logdb] Read index file error %ld", index);
        return kReadError;
    }
    int64_t read_index, entry_offset;
    memcpy(&read_index, buf, 8);
    memcpy(&entry_offset, buf + 8, 8);
    if (read_index != index) {
        LOG(WARNING, "[LogDB] Index file mismatch %ld ", index);
        return kReadError;
    }
    // read log entry
    std::string data;
    {
        MutexLock lock(&mu_);
        if(fseek(log_fp, entry_offset, SEEK_SET) != 0) {
            LOG(WARNING, "[LogDB] Read %ld with invalid offset %ld ", index, entry_offset);
            return kReadError;
        }
        ret = ReadOne(log_fp, &data);
        if (ret == -1) {
            LOG(WARNING, "[LogDB] Read log error %ld ", index);
            return kReadError;
        }
    }
    LogDataEntry log_entry;
    DecodeLogEntry(data, &log_entry);
    if (log_entry.index != index) {
        LOG(WARNING, "[LogDB] Read failed, index mismatch. %ld %ld ", index, log_entry.index);
        return kReadError;
    }
    *entry = log_entry.entry;
    return kOK;
}

StatusCode LogDB::WriteMarkerNoLock(const std::string& key, const std::string& value) {
    std::string data;
    uint32_t len = 4 + key.length() + 4 + value.length();
    data.append(reinterpret_cast<char*>(&len), 4);
    EncodeMarker(MarkerEntry(key, value), &data);
    if (fwrite(data.c_str(), 1, data.length(), marker_log_) != data.length()
        || fflush(marker_log_) != 0) {
        LOG(WARNING, "[LogDB] WriteMarker failed key = %s value = %s", key.c_str(), value.c_str());
        return kWriteError;
    }
    fflush(marker_log_);
    markers_[key] = value;
    return kOK;
}

StatusCode LogDB::WriteMarker(const std::string& key, const std::string& value) {
    MutexLock lock(&mu_);
    return WriteMarkerNoLock(key, value);
}

StatusCode LogDB::WriteMarker(const std::string& key, int64_t value) {
    return WriteMarker(key, std::string(reinterpret_cast<char*>(&value), 8));
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
    MutexLock lock(&mu_);
    *value = largest_index_;
    return kOK;
}

StatusCode LogDB::DeleteUpTo(int64_t index) {
    if (index > largest_index_ || index < smallest_index_) {
        LOG(INFO, "[LogDB] DeleteUpTo over limit index = %ld largest_index_ = %ld",
                index, largest_index_);
        return kBadParameter;
    }
    MutexLock lock(&mu_);
    smallest_index_ = index + 1;
    WriteMarkerNoLock(".smallest_index_", common::NumToString(smallest_index_));
    FileCache::reverse_iterator upto = read_log_.rbegin();
    while (upto != read_log_.rend()) {
        if (upto->first <= index) break;
        ++upto;
    }
    if (upto == read_log_.rend()) {
        return kOK;
    }
    int64_t upto_index = upto->first;
    FileCache::iterator it = read_log_.begin();
    while (it->first != upto_index) {
        fclose((it->second).first);
        fclose((it->second).second);
        std::string log_name, idx_name;
        FormLogName(it->first, &log_name, &idx_name);
        remove(log_name.c_str());
        remove(idx_name.c_str());
        read_log_.erase(it++);
    }
    return kOK;
}

StatusCode LogDB::DeleteFrom(int64_t index) {
    if (index > largest_index_ || index < smallest_index_) {
        LOG(INFO, "[LogDB] DeleteUpTo over limit index = %ld largest_index_ = %ld",
                index, largest_index_);
        return kBadParameter;
    }
    MutexLock lock(&mu_);
    largest_index_ = index - 1;
    FileCache::iterator from = read_log_.lower_bound(index);
    for (FileCache::iterator it = from; it != read_log_.end(); ++it) {
        fclose((it->second).first);
        fclose((it->second).second);
        std::string prefix = dbpath_ + common::NumToString(it->first);
        if (it->first == current_log_index_) {
            fclose(write_log_);
            fclose(write_index_);
            write_log_ = NULL;
            write_index_ = NULL;
        }
        std::string log_name, idx_name;
        FormLogName(it->first, &log_name, &idx_name);
        remove(log_name.c_str());
        remove(idx_name.c_str());
    }
    read_log_.erase(from, read_log_.end());
    if (!read_log_.empty()) {
        FileCache::reverse_iterator it = read_log_.rbegin();
        int offset = 16 * (index - it->first);
        fseek((it->second).first, offset, SEEK_SET);
        char buf[16];
        int len = fread(buf, 1, 16, (it->second).first);
        if (len == 0) {
            return kOK;
        }
        int64_t tmp_offset;
        memcpy(&tmp_offset, buf + 8, 8);
        fclose((it->second).first);
        fclose((it->second).second);
        std::string log_name, idx_name;
        FormLogName(it->first, &log_name, &idx_name);
        truncate(log_name.c_str(), tmp_offset);
        truncate(idx_name.c_str(), offset);
        (it->second).first = fopen(idx_name.c_str(), "r");
        (it->second).second = fopen(log_name.c_str(), "r");
    }
    return kOK;
}

bool LogDB::BuildFileCache() {
    // build log file cache
    struct dirent *entry = NULL;
    DIR *dir_ptr = opendir(dbpath_.c_str());
    if (dir_ptr == NULL) {
        LOG(WARNING, "[LogDB] open dir failed %s", dbpath_.c_str());
        return false;
    }
    while (entry = readdir(dir_ptr)) {
        size_t idx = std::string(entry->d_name).find(".idx");
        if (idx != std::string::npos) {
            std::string file_name = std::string(entry->d_name);
            int64_t index = boost::lexical_cast<int64_t>(file_name.substr(0, idx));
            std::string log_name, idx_name;
            FormLogName(index, &log_name, &idx_name);
            FILE* idx_fp = fopen(idx_name.c_str(), "r");
            if (idx_fp == NULL) {
                LOG(WARNING, "[LogDB] open index file failed %s", file_name.c_str());
                closedir(dir_ptr);
                return false;
            }
            FILE* log_fp = fopen(log_name.c_str(), "r");
            if (log_fp == NULL) {
                LOG(WARNING, "[LogDB] open log file failed %s", file_name.c_str());
                closedir(dir_ptr);
                return false;
            }
            read_log_[index] = std::make_pair(idx_fp, log_fp);
            LOG(INFO, "[LogDB] Add file cache %ld to %s ", index, file_name.c_str());
        }
    }
    closedir(dir_ptr);
    // build largest index
    if (read_log_.empty()) {
        LOG(INFO, "[LogDB] No previous log, largest_index_ = -1");
        return true;
    }
    FileCache::reverse_iterator it = read_log_.rbegin();
    FILE* fp = (it->second).first;
    fseek(fp, 0, SEEK_END);
    int size = ftell(fp);
    largest_index_ = it->first + (size / 16) - 1;
    LOG(INFO, "[LogDB] Set largest_index_ to %ld", largest_index_);
    return true;
}

bool LogDB::RecoverMarker() {
    // recover markers
    FILE* fp = fopen((dbpath_ + "marker.mak").c_str(), "r");
    if (fp == NULL) {
        if (errno == ENOENT) {
            fp = fopen((dbpath_ + "marker.tmp").c_str(), "r");
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
            fclose(fp);
            return false;
        }
        MarkerEntry mark;
        DecodeMarker(data, &mark);
        markers_[mark.key] = mark.value;
    }
    fclose(fp);
    LOG(INFO, "[LogDB] Recover markers done");

    rename((dbpath_ + "marker.tmp").c_str(), (dbpath_ + "marker.mak").c_str());
    return true;
}

void LogDB::WriteMarkerSnapshot() {
    MutexLock lock(&mu_);
    if (marker_log_) fclose(marker_log_);
    FILE* fp = fopen((dbpath_ + "marker.tmp").c_str(), "w");
    if (fp == NULL) {
        LOG(WARNING, "[LogDB] open marker.tmp failed %s", strerror(errno));
        return;
    }
    std::string data;
    for (std::map<std::string, std::string>::iterator it = markers_.begin();
            it != markers_.end(); ++it) {
        MarkerEntry marker(it->first, it->second);
        uint32_t len = 4 + (it->first).length() + 4 + (it->second).length();
        data.clear();
        data.append(reinterpret_cast<char*>(&len), 4);
        EncodeMarker(marker, &data);
        if (fwrite(data.c_str(), 1, data.length(), fp) != data.length() || fflush(fp) != 0) {
            LOG(WARNING, "[LogDB] write marker.tmp failed %s", strerror(errno));
            fclose(fp);
            return;
        }
    }
    fclose(fp);
    rename((dbpath_ + "marker.tmp").c_str(), (dbpath_ + "marker.mak").c_str());
    marker_log_ = fopen((dbpath_ + "marker.mak").c_str(), "a");
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
    if (ret != len) {
        LOG(WARNING, "Read(%d) return %d", len, ret);
        delete[] buf;
        return -1;
    }
    data->clear();
    data->assign(buf, len);
    delete[] buf;
    return len;
}

void LogDB::EncodeLogEntry(const LogDataEntry& log, std::string* data) {
    int64_t index = log.index;
    data->append(reinterpret_cast<char*>(&index), 8);
    data->append(log.entry);
}

void LogDB::DecodeLogEntry(const std::string& data, LogDataEntry* log) { // data = index + log_entry
    memcpy(&(log->index), &(data[0]), 8);
    (log->entry).assign(data.substr(8));
}

void LogDB::EncodeMarker(const MarkerEntry& marker, std::string* data) {
    int klen = (marker.key).length();
    int vlen = (marker.value).length();
    data->append(reinterpret_cast<char*>(&klen), 4);
    data->append(marker.key);
    data->append(reinterpret_cast<char*>(&vlen), 4);
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

bool LogDB::NewWriteLog(int64_t index) {
    std::string log_name, idx_name;
    FormLogName(index, &log_name, &idx_name);
    write_log_ = fopen(log_name.c_str(), "w");
    write_index_ = fopen(idx_name.c_str(), "w");
    FILE* idx_fp = fopen(idx_name.c_str(), "r");
    FILE* log_fp = fopen(log_name.c_str(), "r");
    if (!(write_log_ && write_index_ && idx_fp && log_fp)) {
        LOG(WARNING, "[logdb] open log/idx file failed %ld %s", index, strerror(errno));
        return false;
    }
    read_log_[index] = std::make_pair(idx_fp, log_fp);
    current_log_index_ = index;
    return true;
}

void LogDB::FormLogName(int64_t index, std::string* log_name, std::string* idx_name) {
    log_name->clear();
    log_name->append(dbpath_);
    log_name->append(common::NumToString(index));
    log_name->append(".log");

    idx_name->clear();
    idx_name->append(dbpath_);
    idx_name->append(common::NumToString(index));
    idx_name->append(".idx");
}

} // namespace bfs
} // namespace baidu
