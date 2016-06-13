// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//

#ifndef  BFS_NAMESERVER_LOGDB_H_
#define  BFS_NAMESERVER_LOGDB_H_

#include <string>

#include "proto/status_code.pb.h"

namespace baidu {
namespace bfs {

class LogDB {
public:
    LogDB(const std::string& db_path);
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
};

} // namespace bfs
} // namespace baidu

#endif  //BFS_NAMESERVER_LOGDB_H_
