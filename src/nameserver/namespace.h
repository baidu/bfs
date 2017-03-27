// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#ifndef  BFS_NAMESPACE_H_
#define  BFS_NAMESPACE_H_

#include <stdint.h>
#include <string>
#include <functional>
#include <common/mutex.h>

#include <leveldb/db.h>

#include "proto/nameserver.pb.h"
#include "proto/status_code.pb.h"



namespace baidu {
namespace bfs {

class NameSpace {
public:
    NameSpace(bool standalone = true);
    void Activate(std::function<void (const std::vector<FileInfo>&)> rebuild_callback, NameServerLog* log);
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
    /// Symlink: dst -> src
    StatusCode Symlink(const std::string& src,
                       const std::string& dst,
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
    bool RebuildBlockMap(std::function<void (const std::vector<FileInfo>&)> callback);
    /// NormalizePath
    static std::string NormalizePath(const std::string& path);
    /// ha - tail log from leader/master
    void TailLog(const std::string& log);
    void TailSnapshot(int32_t ns_id, std::string* logstr);
    void EraseNamespace();
    int64_t GetNewBlockId();
    StatusCode GetDirLockStatus(const std::string& path);
    void SetDirLockStatus(const std::string& path, StatusCode status,
                          const std::string& uuid = "");
    void ListAllBlocks(const std::string& path, std::vector<int64_t>* result);
private:
    enum FileType {
        kDefault = 0,
        kDir = 1,
        kSymlink = 2,
    };
    FileType GetFileType(int type) const;
    bool GetLinkSrcPath(const FileInfo& info, FileInfo* src_info);
    StatusCode BuildPath(const std::string& path, FileInfo* file_info, std::string* fname,
                                NameServerLog* log = NULL);
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
    void InitBlockIdUpbound(NameServerLog* log);
    void UpdateBlockIdUpbound(NameServerLog* log);
private:
    leveldb::DB* db_;   /// NameSpace storage
    leveldb::Cache* db_cache_;  // block cache for leveldb
    int64_t version_;   /// Namespace version.
    volatile int64_t last_entry_id_;
    FileInfo root_path_;
    int64_t block_id_upbound_;
    int64_t next_block_id_;
    Mutex mu_;

    /// HA module
    std::map<int32_t, leveldb::Iterator*> snapshot_tasks_;

private:
    // NameSpace should be noncopyable
    NameSpace(const NameSpace&);
    NameSpace& operator=(const NameSpace&);
};

} // namespace bfs
} //namespace baidu

#endif  // BFS_NAMESPACE_H_

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
