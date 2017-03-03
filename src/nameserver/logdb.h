// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//

#ifndef  BFS_NAMESERVER_LOGDB_H_
#define  BFS_NAMESERVER_LOGDB_H_

#include <string>
#include <map>
#include <stdio.h>

#include <common/mutex.h>
#include <common/thread_pool.h>

#include "proto/status_code.pb.h"

namespace baidu {
namespace bfs {

struct DBOption {
    int64_t snapshot_interval; // write marker snapshot interval, in seconds
    int64_t log_size;
    DBOption() : snapshot_interval(60), log_size(128) /* in MB */ {}
};

struct MarkerEntry { // entry_length + key_len + key + value_len + value
    std::string key;
    std::string value;
    MarkerEntry() {}
    MarkerEntry(const std::string& key, const std::string& value) : key(key), value(value) {}
};

class LogDB {
public:
    LogDB();
    ~LogDB();
    static void Open(const std::string& path, const DBOption& option, LogDB** dbptr);
    StatusCode Write(int64_t index, const std::string& entry);
    // Read log entry
    StatusCode Read(int64_t index, std::string* entry);

    // Write marker.
    StatusCode WriteMarker(const std::string& key, const std::string& value);
    StatusCode WriteMarker(const std::string& key, int64_t value);
    // Read marker. Return empty string if cannot find 'key'
    StatusCode ReadMarker(const std::string& key, std::string* value);
    // Read marker. Return -1 string if cannot find 'key'
    StatusCode ReadMarker(const std::string& key, int64_t* value);

    // Return the largest index in logdb. Return -1 if db is empty.
    StatusCode GetLargestIdx(int64_t* value);
    // delete all entries smaller than or equal to 'index'
    StatusCode DeleteUpTo(int64_t index);
    // delete all entries larter than or equal to 'index'
    StatusCode DeleteFrom(int64_t index);

    // Delete all data in db
    static StatusCode DestroyDB(const std::string& dbpath);

    /// for dumper ///
    static int ReadOne(FILE* fp, std::string* data);
    static StatusCode ReadIndex(FILE* fp, int64_t expect_index, int64_t* index, int64_t* offset);
    static void DecodeMarker(const std::string& data, MarkerEntry* marker);
private:
    bool RecoverMarker();
    bool BuildFileCache();
    bool CheckLogIdx();
    void WriteMarkerSnapshot();
    void CloseCurrent();
    void EncodeMarker(const MarkerEntry& marker, std::string* data);
    bool NewWriteLog(int64_t index);
    void FormLogName(int64_t index, std::string* log_name, std::string* idx_name);
    StatusCode WriteMarkerNoLock(const std::string& key, const std::string& value);
    bool CloseFile(FILE* fp, const std::string& hint);
    bool OpenFile(FILE** fp, const std::string& name, const char* mode);
    bool RemoveFile(const std::string& name);
    bool RemoveFile(FILE* fp, const std::string& name);
private:
    Mutex mu_;
    ThreadPool* thread_pool_;

    std::string dbpath_;
    int64_t snapshot_interval_;
    int64_t log_size_;
    std::map<std::string, std::string> markers_;
    int64_t next_index_; // smallest_index_ <= db < largest_index_
    int64_t smallest_index_;    // smallest index in db, -1 indicates empty db

    typedef std::map<int64_t, std::pair<FILE*, FILE*> > FileCache;
    FILE* write_log_;       // log file ends with '.log'
    FILE* write_index_;     // index file ends with '.idx'
    FileCache read_log_;    // file cache, index -> (idx_fp, log_fp)
    FILE* marker_log_;      // marker file names 'marker.mak'
};

} // namespace bfs
} // namespace baidu

#endif  //BFS_NAMESERVER_LOGDB_H_
