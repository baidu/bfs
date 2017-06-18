// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef  TERA_LEVELDB_DFS_H_
#define  TERA_LEVELDB_DFS_H_

#include <stdint.h>
#include <string>
#include <vector>

namespace leveldb {

class DfsFile {
public:
    DfsFile() {}
    virtual ~DfsFile() {}
    /// Returns the number of bytes written, -1 on error.
    virtual int32_t Write(const char* buf, int32_t len) = 0;
    /// Returns 0 on success.
    virtual int32_t Flush() = 0;
    /// Returns 0 on success.
    virtual int32_t Sync() = 0;
    /// Returns the number of bytes actually read, possibly less
    /// than than length;-1 on error.
    virtual int32_t Read(char* buf, int32_t len) = 0;
    /// Returns the number of bytes actually read, possibly less than
    /// than length;-1 on error.
    virtual int32_t Pread(int64_t offset, char* buf, int32_t len) = 0;
    /// Return Current offset.
    virtual int64_t Tell() = 0;
    /// Returns 0 on success.
    virtual int32_t Seek(int64_t offset) = 0;
    /// Returns 0 on success.
    virtual int32_t CloseFile() = 0;
private:
    DfsFile(const DfsFile&);
    void operator=(const DfsFile&);
};

enum open_type {
  RDONLY = 1,
  WRONLY = 2
};

class Dfs {
public:
    Dfs() {}
    virtual ~Dfs() {}
    /// Returns 0 on success.
    virtual int32_t CreateDirectory(const std::string& path) = 0;
    /// Returns 0 on success.
    virtual int32_t DeleteDirectory(const std::string& path) = 0;
    /// Returns 0 on success.
    virtual int32_t Exists(const std::string& filename) = 0;
    /// Returns 0 on success.
    virtual int32_t Delete(const std::string& filename) = 0;
    /// Returns 0 on success.
    virtual int32_t GetFileSize(const std::string& filename, uint64_t* size) = 0;
    /// Returns 0 on success.
    virtual int32_t Rename(const std::string& from, const std::string& to) = 0;
    /// Returns 0 on success.
    virtual int32_t Copy(const std::string& from, const std::string& to) = 0;
    /// Returns 0 on success.
    virtual int32_t ListDirectory(const std::string& path,
                                  std::vector<std::string>* result) = 0;
    /// Returns 0 on success.
    virtual int32_t LockDirectory(const std::string& path) = 0;
    /// Returns 0 on success.
    virtual int32_t UnlockDirectory(const std::string& path) = 0;
    /// Returns DfsFile handler on success, NULL on error.WithTime
    virtual DfsFile* OpenFile(const std::string& filename, int32_t flags) = 0;
    /// Returns Dfs handler on success, NULL on error.
    static Dfs* NewDfs(const std::string& so_path, const std::string& conf);
private:
    Dfs(const Dfs&);
    void operator=(const Dfs&);
};

/// Dfs creator type.
typedef Dfs* (*DfsCreator)(const char*);

/// Implement your own dfs like this:
/// class MyDfsFile : public DfsFile {
///     ...
/// };
/// class MyDfs : public Dfs {
///     ...
/// };
///
/// extern "C" {
/// Dfs* NewDfs(const char* conf) {
///     Dfs* dfs= new MyDfs();
///     ...
///     return dfs;
/// }
/// }

} // namespace leveldb
#endif  //TERA_LEVELDB_DFS_H_

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
