// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#ifndef  BFS_FILE_CACHE_H_
#define  BFS_FILE_CACHE_H_

#include <common/cache.h>

namespace bfs {

class FileCache {
public:
    FileCache(int32_t cache_size);
    ~FileCache();
    int32_t ReadFile(const std::string& file_path, char* buf, int32_t count, int64_t offset);
    void EraseFileCache(const std::string& file_path);
private:
    common::Cache::Handle* FindFile(const std::string& file_path);
private:
    common::Cache* _cache;
};

} // namespace bfs

#endif  // BFS_FILE_CACHE_H_

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
