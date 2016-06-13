// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//

#include "nameserver/logdb.h"

namespace baidu {
namespace bfs {

LogDB::LogDB(const std::string& db_path) {

}

StatusCode LogDB::Write(int64_t index, const std::string& entry) {
    return kOK;
}

StatusCode LogDB::Read(int64_t index, std::string* entry) {
    return kOK;
}

StatusCode LogDB::WriteMarker(const std::string& key, const std::string& value) {
    return kOK;
}

StatusCode LogDB::WriteMarker(const std::string& key, int64_t value) {
    return kOK;
}

StatusCode LogDB::ReadMarker(const std::string& key, std::string* value) {
    return kOK;
}

StatusCode LogDB::ReadMarker(const std::string& key, int64_t* value) {
    return kOK;
}

StatusCode LogDB::GetLargestIdx(int64_t* value) {
    return kOK;
}

void LogDB::DeleteUpTo(int64_t index) {

}

void LogDB::DeleteFrom(int64_t index) {

}

} // namespace bfs
} // namespace baidu
