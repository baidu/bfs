// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#ifndef  BFS_NAMESPACE_H_
#define  BFS_NAMESPACE_H_

#include <stdint.h>
#include <string>
#include <boost/function.hpp>

#include <leveldb/db.h>

#include "proto/nameserver.pb.h"
#include "proto/status_code.pb.h"

namespace baidu {
namespace bfs {

class NameSpace {
public:
    NameSpace();
    ~NameSpace();
    /// List a directory
    StatusCode ListDirectory(const std::string& path, int32_t user_id,
                      google::protobuf::RepeatedPtrField<FileInfo>* outputs);
    /// Create file by name
    StatusCode CreateFile(const std::string& file_name, int flags, int mode, int replica_num,
                   int32_t user_id = 0);
    /// Remove file by name
    StatusCode RemoveFile(const std::string& path, int32_t user_id, FileInfo* file_removed);
    /// Remove director.
    StatusCode DeleteDirectory(const std::string& path, bool recursive, int32_t user_id,
                       std::vector<FileInfo>* files_removed);
    /// File rename
    StatusCode Rename(const std::string& old_path,
               const std::string& new_path,
               int32_t user_id,
               bool* need_unlink,
               FileInfo* remove_file);
    /// Get file
    bool GetFileInfo(const std::string& path, int32_t user_id, FileInfo* file_info);
    /// Update file
    bool UpdateFileInfo(const FileInfo& file_info);
    /// Delete file
    bool DeleteFileInfo(const std::string file_key);
    /// Namespace version
    int64_t Version() const;
    /// Rebuild blockmap
    bool RebuildBlockMap(boost::function<void (const FileInfo&)> callback);
    /// NormalizePath
    static std::string NormalizePath(const std::string& path);
private:
    bool CheckPermission(const FileInfo& file_info, int32_t user_id, int op);
    static bool IsDir(int type);
    static void EncodingStoreKey(int64_t entry_id,
                          const std::string& path,
                          std::string* key_str);
    bool GetFromStore(const std::string& key, FileInfo* info);
    void SetupRoot();
    bool LookUp(const std::string& path, int32_t user_id, FileInfo* info);
    bool LookUp(int64_t pid, const std::string& name, FileInfo* info);
    StatusCode InternalDeleteDirectory(const FileInfo& dir_info,
                                bool recursive, int32_t user_id,
                                std::vector<FileInfo>* files_removed);
private:
    leveldb::DB* db_;   /// NameSpace storage
    int64_t version_;   /// Namespace version.
    volatile int64_t last_entry_id_;
    FileInfo root_path_;
};

} // namespace bfs
} //namespace baidu

#endif  // BFS_NAMESPACE_H_

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
