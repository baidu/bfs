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
#include <map>
#include <vector>

#include "proto/status_code.pb.h"

namespace baidu {
namespace bfs {

enum Status {
    OK = 0,
    NotFound = 1,
    NotFile = 2,
    NotDirectory = 3,
};

std::string GetErrorCodeString(int32_t error_code) {
    assert(error_code < 0);
    return StatusCode_Name(static_cast<StatusCode>(error_code));
}

/// Bfs File interface
class File {
public:
    File() {}
    virtual ~File() {}
    virtual int32_t Pread(char* buf, int32_t read_size, int64_t offset, bool reada = false) = 0;
    virtual int64_t Seek(int64_t offset, int32_t whence) = 0;
    virtual int32_t Read(char* buf, int32_t read_size) = 0;
    virtual int32_t Write(const char* buf, int32_t write_size) = 0;
    virtual int32_t Flush() = 0;
    virtual int32_t Sync(int32_t timeout = 0) = 0;
    virtual int32_t Close() = 0;
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
    virtual int32_t CreateDirectory(const char* path) = 0;
    /// List Directory
    virtual int32_t ListDirectory(const char* path, BfsFileInfo** filelist, int *num) = 0;
    /// Delete Directory
    virtual int32_t DeleteDirectory(const char* path, bool recursive) = 0;
    /// Access
    virtual int32_t Access(const char* path, int32_t mode) = 0;
    /// Stat
    virtual int32_t Stat(const char* path, BfsFileInfo* fileinfo) = 0;
    /// GetFileSize: get real file size
    virtual int32_t GetFileSize(const char* path, int64_t* file_size) = 0;
    /// Open file for read or write, flags: O_WRONLY or O_RDONLY
    virtual int32_t OpenFile(const char* path, int32_t flags, File** file) = 0;
    virtual int32_t OpenFile(const char* path, int32_t flags, int32_t mode,
                          int replica, File** file) = 0;
    virtual int32_t CloseFile(File* file) = 0;
    virtual int32_t DeleteFile(const char* path) = 0;
    virtual int32_t Rename(const char* oldpath, const char* newpath) = 0;
    virtual int32_t ChangeReplicaNum(const char* file_name, int32_t replica_num) = 0;

    /// Show system status
    virtual int32_t SysStat(const std::string& stat_name, std::string* result) = 0;
    /// GetFileLocation: get file locate, return chunkserver address and port
    virtual int32_t GetFileLocation(const std::string& path,
                                 std::map<int64_t, std::vector<std::string> >* locations) = 0;
    virtual int32_t ShutdownChunkServer(const std::vector<std::string>& cs_address) = 0;
    virtual int32_t ShutdownChunkServerStat() = 0;
private:
    // No copying allowed
    FS(const FS&);
    void operator=(const FS&);
};

} // namespace bfs
} // namespace baidu

#endif  // LIBBFS_BFS_H_

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
