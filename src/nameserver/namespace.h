// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#ifndef  BFS_NAMESPACE_H_
#define  BFS_NAMESPACE_H_

#include <stdint.h>
#include <string>
#include <common/mutex.h>
#include <boost/function.hpp>

#include <leveldb/db.h>

#include "proto/nameserver.pb.h"
#include "proto/status_code.pb.h"

namespace baidu {
namespace bfs {

class NameSpace {
public:
    NameSpace(bool standalone = true);
    void Activate(boost::function<void (const FileInfo&)> rebuild_callback, NameServerLog* log);
    ~NameSpace();
    /// List a directory
    StatusCode ListDirectory(const std::string& path,
                      google::protobuf::RepeatedPtrField<FileInfo>* outputs);
    /// Create file by name
    StatusCode CreateFile(const std::string& file_name, int flags, int mode,
                          int replica_num, std::vector<int64_t>* blocks_to_remove,
                          NameServerLog* log = NULL);
    /// Remove file by name
    StatusCode RemoveFile(const std::string& path, FileInfo* file_removed, NameServerLog* log = NULL);
    /// Remove director.
    StatusCode DeleteDirectory(const std::string& path, bool recursive,
                        std::vector<FileInfo>* files_removed, NameServerLog* log = NULL);
    StatusCode DiskUsage(const std::string& path, uint64_t* du_size);
    /// File rename
    StatusCode Rename(const std::string& old_path,
               const std::string& new_path,
               bool* need_unlink,
               FileInfo* remove_file,
               NameServerLog* log = NULL);
    /// Get file
    bool GetFileInfo(const std::string& path, FileInfo* file_info);
    /// Update file
    bool UpdateFileInfo(const FileInfo& file_info, NameServerLog* log = NULL);
    /// Delete file
    bool DeleteFileInfo(const std::string file_key, NameServerLog* log = NULL);
    /// Namespace version
    int64_t Version() const;
    /// Rebuild blockmap
    bool RebuildBlockMap(boost::function<void (const FileInfo&)> callback);
    /// NormalizePath
    static std::string NormalizePath(const std::string& path);
    /// ha - tail log from leader/master
    void TailLog(const std::string& log);
    int64_t GetNewBlockId(NameServerLog* log);
    void InitBlockIdUpbound(NameServerLog* log);
private:
    static bool IsDir(int type);
    static void EncodingStoreKey(int64_t entry_id,
                          const std::string& path,
                          std::string* key_str);
    static void DecodingStoreKey(const std::string& key_str,
                                 int64_t* entry_id,
                                 std::string* path);
    bool GetFromStore(const std::string& key, FileInfo* info);
    void SetupRoot();
    bool LookUp(const std::string& path, FileInfo* info);
    bool LookUp(int64_t pid, const std::string& name, FileInfo* info);
    StatusCode InternalDeleteDirectory(const FileInfo& dir_info,
                                bool recursive,
                                std::vector<FileInfo>* files_removed,
                                NameServerLog* log);
    StatusCode InternalComputeDiskUsage(const FileInfo& info, uint64_t* du_size);
    uint32_t EncodeLog(NameServerLog* log, int32_t type,
                       const std::string& key, const std::string& value);
    void UpdateBlockIdUpbound(NameServerLog* log);
private:
    leveldb::DB* db_;   /// NameSpace storage
    int64_t version_;   /// Namespace version.
    volatile int64_t last_entry_id_;
    FileInfo root_path_;
    int64_t block_id_upbound_;
    int64_t next_block_id_;
    Mutex mu_;

    /// HA module
    //Sync* sync_;
    //Mutex mu_;
};

} // namespace bfs
} //namespace baidu

#endif  // BFS_NAMESPACE_H_

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
