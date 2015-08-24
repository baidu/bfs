// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com


#include "file_cache.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "common/logging.h"

namespace bfs {

struct FileEntity {
    int32_t fd;
    std::string file_name;
};

static void DeleteEntry(const common::Slice& key, void* value) {
    FileEntity* file = reinterpret_cast<FileEntity*>(value);
    close(file->fd);
    LOG(INFO, "FileCache Close file %s", file->file_name.c_str());
    delete file;
}

FileCache::FileCache(int32_t cache_size)
    : _cache(common::NewLRUCache(cache_size)) {
}

FileCache::~FileCache() {
    delete _cache;
}

common::Cache::Handle* FileCache::FindFile(const std::string& file_path) {
    common::Slice key(file_path);
    common::Cache::Handle* handle = _cache->Lookup(key);
    if (handle == NULL) {
        int fd = open(file_path.c_str(), O_RDONLY);
        if (fd < 0) {
            return NULL;
        }
        FileEntity* file = new FileEntity;
        file->fd = fd;
        file->file_name = file_path;
        handle = _cache->Insert(key, file, 1, &DeleteEntry);
    }
    return handle;
}
int32_t FileCache::ReadFile(const std::string& file_path, char* buf,
                            int32_t len, int64_t offset) {
    common::Cache::Handle* handle = FindFile(file_path);
    int32_t fd = (reinterpret_cast<FileEntity*>(_cache->Value(handle)))->fd;
    int32_t ret = pread(fd, buf, len, offset);
    _cache->Release(handle);
    return ret;
}

void FileCache::EraseFileCache(const std::string& file_path) {
    _cache->Erase(file_path);
}

} // namespace bfs
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
