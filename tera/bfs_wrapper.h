// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#include <bfs.h>
#include <string>
#include <vector>

#include "dfs.h"

namespace baidu {
namespace bfs {

class BfsFile : public leveldb::DfsFile {
public:
    BfsFile(const std::string& name, File* file) : name_(name), file_(file) {}
    /// Returns the number of bytes written, -1 on error.
    virtual int32_t Write(const char* buf, int32_t len);
    /// Returns 0 on success.
    virtual int32_t Flush();
    /// Returns 0 on success.
    virtual int32_t Sync();
    /// Returns the number of bytes actually read, possibly less
    /// than than length;-1 on error.
    virtual int32_t Read(char* buf, int32_t len);
    /// Returns the number of bytes actually read, possibly less than
    /// than length;-1 on error.
    virtual int32_t Pread(int64_t offset, char* buf, int32_t len);
    /// Return Current offset.
    virtual int64_t Tell();
    /// Returns 0 on success.
    virtual int32_t Seek(int64_t offset);
    /// Returns 0 on success.
    virtual int32_t CloseFile();
private:
    std::string name_;
    File* file_;
};

class BfsImpl : public leveldb::Dfs {
public:
    BfsImpl(const std::string& conf);
    /// Returns 0 on success.
    virtual int32_t CreateDirectory(const std::string& path);
    /// Returns 0 on success.
    virtual int32_t DeleteDirectory(const std::string& path);
    /// Returns 0 on success.
    virtual int32_t Exists(const std::string& filename);
    /// Returns 0 on success.
    virtual int32_t Delete(const std::string& filename);
    /// Returns 0 on success.
    virtual int32_t GetFileSize(const std::string& filename, uint64_t* size);
    /// Returns 0 on success.
    virtual int32_t Rename(const std::string& from, const std::string& to);
    /// Returns 0 on success.
    virtual int32_t Copy(const std::string& from, const std::string& to);
    /// Returns 0 on success.
    virtual int32_t ListDirectory(const std::string& path, std::vector<std::string>* result);
    /// Returns DfsFile handler on success, NULL on error.
    virtual leveldb::DfsFile* OpenFile(const std::string& filename, int32_t flags);
private:
    bfs::FS* fs_;
};

}
} // namespace

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
