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

#include <common/logging.h>

namespace baidu {
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
    : cache_(common::NewLRUCache(cache_size)) {
}

FileCache::~FileCache() {
    delete cache_;
}

common::Cache::Handle* FileCache::FindFile(const std::string& file_path) {
    common::Slice key(file_path);
    common::Cache::Handle* handle = cache_->Lookup(key);
    if (handle == NULL) {
        int fd = open(file_path.c_str(), O_RDONLY);
        if (fd < 0) {
            LOG(WARNING, "File not found: %s", file_path.c_str());
            return NULL;
        }
        FileEntity* file = new FileEntity;
        file->fd = fd;
        file->file_name = file_path;
        handle = cache_->Insert(key, file, 1, &DeleteEntry);
    }
    return handle;
}

int64_t FileCache::ReadFile(const std::string& file_path, char* buf,
                            int64_t len, int64_t offset) {
    common::Cache::Handle* handle = FindFile(file_path);
    if (handle == NULL) {
        return -1;
    }
    int32_t fd = (reinterpret_cast<FileEntity*>(cache_->Value(handle)))->fd;
    int64_t ret = pread(fd, buf, len, offset);
    cache_->Release(handle);
    return ret;
}

void FileCache::EraseFileCache(const std::string& file_path) {
    cache_->Erase(file_path);
}

} // namespace bfs
} //namespace baidu
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
