// Copyright (c) 2014, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#ifndef  LIBBFS_BFS_H_
#define  LIBBFS_BFS_H_

#include <stdint.h>
#include <fcntl.h>
#include <string>

namespace baidu {
namespace bfs {

enum Status {
    OK = 0,
    NotFound = 1,
    NotFile = 2,
    NotDirectory = 3,
};

/// Bfs File interface
class File {
public:
    File() {}
    virtual ~File() {}
    virtual int32_t Pread(char* buf, int32_t read_size, int64_t offset, bool reada = false) = 0;
    virtual int64_t Seek(int64_t offset, int32_t whence) = 0;
    virtual int32_t Read(char* buf, int32_t read_size) = 0;
    virtual int32_t Write(const char* buf, int32_t write_size) = 0;
    virtual bool Flush() = 0;
    virtual bool Sync(int32_t timeout = 0) = 0;
    virtual bool Close() = 0;
private:
    // No copying allowed
    File(const File&);
    void operator=(const File&);
};

struct BfsFileInfo {
    int64_t size;
    uint32_t ctime;
    uint32_t mode;
    char name[1024];
};

// Bfs fileSystem interface
class FS {
public:
    FS() { }
    virtual ~FS() { }
    /// Open filesystem with nameserver address (host:port)
    static bool OpenFileSystem(const char* nameserver, FS** fs);
    /// Create directory
    virtual bool CreateDirectory(const char* path) = 0;
    /// List Directory
    virtual bool ListDirectory(const char* path, BfsFileInfo** filelist, int *num) = 0;
    /// Delete Directory
    virtual bool DeleteDirectory(const char* path, bool recursive) = 0;
    /// Access
    virtual bool Access(const char* path, int32_t mode) = 0;
    /// Stat
    virtual bool Stat(const char* path, BfsFileInfo* fileinfo) = 0;
    /// GetFileSize: get real file size
    virtual bool GetFileSize(const char* path, int64_t* file_size) = 0;
    /// Open file for read or write, flags: O_WRONLY or O_RDONLY
    virtual bool OpenFile(const char* path, int32_t flags, File** file) = 0;
    virtual bool CloseFile(File* file) = 0;
    virtual bool DeleteFile(const char* path) = 0;
    virtual bool Rename(const char* oldpath, const char* newpath) = 0;
    virtual bool ChangeReplicaNum(const char* file_name, int32_t replica_num) = 0;

    /// Show system status
    virtual bool SysStat(const std::string& stat_name, std::string* result) = 0;
private:
    // No copying allowed
    FS(const FS&);
    void operator=(const FS&);
};

} // namespace bfs
} // namespace baidu

#endif  // LIBBFS_BFS_H_

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
