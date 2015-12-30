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

namespace baidu {
namespace bfs {

class NameSpace {
public:
    NameSpace();
    ~NameSpace();
    /// List a directory
    int ListDirectory(const std::string& path,
                      google::protobuf::RepeatedPtrField<FileInfo>* outputs);
    /// Create file by name
    int CreateFile(const std::string& file_name, int flags, int mode);
    /// Remove file by name
    int RemoveFile(const std::string& path, FileInfo* file_removed);
    /// Remove director.
    int DeleteDirectory(const std::string& path, bool recursive,
                        std::vector<FileInfo>* files_removed);
    /// File rename
    int Rename(const std::string& old_path,
               const std::string& new_path,
               bool* need_unlink,
               FileInfo* remove_file);
    /// Own iterator?
    leveldb::Iterator* NewIterator();
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
private:
    static bool IsDir(int type);
    static void EncodingStoreKey(int64_t entry_id,
                          const std::string& path,
                          std::string* key_str);
    bool GetFromStore(const std::string& key, FileInfo* info);
    bool LookUp(const std::string& path, FileInfo* info);
    bool LookUp(int64_t pid, const std::string& name, FileInfo* info);
    int InternalDeleteDirectory(const FileInfo& dir_info,
                                bool recursive,
                                std::vector<FileInfo>* files_removed);
private:
    leveldb::DB* _db;   /// NameSpace storage
    int64_t _version;   /// Namespace version.
    volatile int64_t _last_entry_id;
};

} // namespace bfs
} //namespace baidu

#endif  // BFS_NAMESPACE_H_

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
