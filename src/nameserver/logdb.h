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

struct LogDataEntry // entry_length + index + log
{
    int64_t index;
    std::string entry;
    LogDataEntry() : index(0) {}
    LogDataEntry(int64_t index, const std::string& entry) : index(index), entry(entry) {}
};

struct MarkerEntry // entry_length + key_len + key + value_len + value
{
    std::string key;
    std::string value;
    MarkerEntry() {}
    MarkerEntry(const std::string& key, const std::string& value) : key(key), value(value) {}
};

class LogDB {
public:
    LogDB(const std::string& db_path);
    ~LogDB();
    // Write log entry
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
    void DeleteUpTo(int64_t index);
    // delete all entries larter than or equal to 'index'
    void DeleteFrom(int64_t index);

    /// for dumper ///
    static int ReadOne(FILE* fp, std::string* data);
    static void DecodeLogEntry(const std::string& data, LogDataEntry* log);
    static void DecodeMarker(const std::string& data, MarkerEntry* marker);
private:
    bool Init();
    bool RecoverMarker();
    void WriteMarkerSnapshot();
    void EncodeLogEntry(const LogDataEntry& log, std::string* data);
    void EncodeMarker(const MarkerEntry& marker, std::string* data);
private:
    Mutex mu_;
    ThreadPool* thread_pool_;
    std::string dbpath_;
    FILE* write_log_;   // log file ends with '.log'
    FILE* read_log_;
    FILE* write_index_; // index file ends with '.idx'
    FILE* read_index_;
    FILE* marker_log_;  // marker file names 'marker.mak'
    std::map<std::string, std::string> markers_;
};

} // namespace bfs
} // namespace baidu

#endif  //BFS_NAMESERVER_LOGDB_H_
