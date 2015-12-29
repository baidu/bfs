// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#ifndef  BFS_NAMESPACE_H_
#define  BFS_NAMESPACE_H_

#include <stdint.h>
#include <string>

#include <leveldb/db.h>

#include "proto/nameserver.pb.h"

namespace baidu {
namespace bfs {

class NameSpace {
public:
    NameSpace();
    /// List a directory
    int ListDirectory(const std::string& path,
                      google::protobuf::RepeatedPtrField<FileInfo>* outputs);
    /// Create file by name
    int CreateFile(const std::string& file_name, int flags, int mode);
    /// Remove file by name
    int RemoveFile(const std::string& path, FileInfo* file_removed);
    /// Remove director.
    int DeleteDirectory(std::string& path, bool recursive,
                        std::vector<FileInfo>* files_removed);
    /// File rename
    int Rename(const std::string& old_path,
               const std::string& new_path,
               bool* need_unlink,
               FileInfo* remove_file);
    /// Own iterator?
    leveldb::Iterator* NewIterator();
    /// Get file
    bool GetFileInfo(const std::string& path,
                     FileInfo* file_info,
                     std::string* file_key);
    /// Update file
    bool UpdateFileInfo(const std::string& file_key, FileInfo& file_info);
    /// Delete file
    bool DeleteFileInfo(const std::string file_key);
    /// Namespace version
    int64_t Version() const;
private:
    leveldb::DB* _db;
    int64_t _version;   /// Namespace version.
};

} // namespace bfs
} //namespace baidu

#endif  // BFS_NAMESPACE_H_

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
