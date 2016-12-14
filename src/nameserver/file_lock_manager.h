// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//

#ifndef  BFS_FILE_LOCK_MANAGER_H_
#define  BFS_FILE_LOCK_MANAGER_H_

#include <string>
#include <unordered_map>
#include <vector>

#include <common/rw_lock.h>
#include <common/counter.h>
#include <common/mutex.h>
#include <common/logging.h>

namespace baidu {
namespace bfs {

class FileLockManager {
public:
    FileLockManager(int bucket_num = 19);
    ~FileLockManager();
    void ReadLock(const std::string& file_path);
    void WriteLock(const std::string& file_path);
    void Unlock(const std::string& file_path);
private:
    enum LockType {
        kRead,
        kWrite
    };
    struct LockEntry {
        common::Counter ref_;
        common::RWLock rw_lock_;
    };
    struct LockBucket {
        Mutex mu;
        std::unordered_map<std::string, LockEntry*> lock_map;
    };
    void LockInternal(const std::string& path, LockType lock_type);
    void UnlockInternal(const std::string& path);
    int GetBucketOffset(const std::string& path);
    std::vector<LockBucket*> locks_;
};

} // namespace bfs
} // namespace baidu

#endif
