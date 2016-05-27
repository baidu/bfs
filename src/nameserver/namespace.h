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
#include <common/mutex.h>

#include "proto/nameserver.pb.h"
#include "proto/status_code.pb.h"

namespace baidu {
namespace bfs {

class NameSpace {
public:
    NameSpace();
    ~NameSpace();
    /// List a directory
    StatusCode ListDirectory(const std::string& path,
                      google::protobuf::RepeatedPtrField<FileInfo>* outputs);
    /// Create file by name
    StatusCode CreateFile(const std::string& file_name, int flags, int mode, int replica_num);
    /// Remove file by name
    StatusCode RemoveFile(const std::string& path, FileInfo* file_removed);
    /// Remove director.
    StatusCode DeleteDirectory(const std::string& path, bool recursive,
                        std::vector<FileInfo>* files_removed);
    /// File rename
    StatusCode Rename(const std::string& old_path,
               const std::string& new_path,
               bool* need_unlink,
               FileInfo* remove_file);
    /// Get file
    bool GetFileInfo(const std::string& path, FileInfo* file_info);
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
    int64_t GetNewBlockId();
private:
    static bool IsDir(int type);
    static void EncodingStoreKey(int64_t entry_id,
                          const std::string& path,
                          std::string* key_str);
    bool GetFromStore(const std::string& key, FileInfo* info);
    void SetupRoot();
    bool LookUp(const std::string& path, FileInfo* info);
    bool LookUp(int64_t pid, const std::string& name, FileInfo* info);
    StatusCode InternalDeleteDirectory(const FileInfo& dir_info,
                                bool recursive,
                                std::vector<FileInfo>* files_removed);
    void UpdateBlockIdUpbound();
private:
    leveldb::DB* db_;   /// NameSpace storage
    int64_t version_;   /// Namespace version.
    volatile int64_t last_entry_id_;
    FileInfo root_path_;
    int64_t next_block_id_;
    int64_t block_id_upbound_;
    Mutex mu_;
};

} // namespace bfs
} //namespace baidu

#endif  // BFS_NAMESPACE_H_

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
