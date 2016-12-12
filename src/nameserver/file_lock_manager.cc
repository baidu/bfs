/* // Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved. */
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//

#include "file_lock_manager.h"

#include <vector>
#include <string.h>
#include <assert.h>

#include <common/string_util.h>

namespace baidu {
namespace bfs {

void FileLockManager::ReadLock(const std::string& file_path) {
    LOG(DEBUG, "Try get read lock for %s", file_path.c_str());
    std::vector<std::string> paths;
    common::SplitString(file_path, "/", &paths);
    //first lock "/"
    LockInternal("/", kRead);
    std::string cur_path;
    for (size_t i = 0; i < paths.size(); i++) {
        cur_path += ("/" + paths[i]);
        LockInternal(cur_path, kRead);
    }
}

void FileLockManager::WriteLock(const std::string& file_path) {
    LOG(DEBUG, "Try get write lock for %s", file_path.c_str());
    std::vector<std::string> paths;
    common::SplitString(file_path, "/", &paths);
    //first lock "/"
    if (paths.size() == 0) {
        LockInternal("/", kWrite);
        return;
    }
    LockInternal("/", kRead);
    std::string cur_path;
    for (size_t i = 0; i < paths.size() - 1; i++) {
        cur_path += ("/" + paths[i]);
        LockInternal(cur_path, kRead);
    }
    cur_path += ("/" + paths.back());
    LockInternal(cur_path, kWrite);
}

void FileLockManager::Unlock(const std::string& file_path) {
    /// TODO maybe use NormalizePath is better
    std::vector<std::string> paths;
    common::SplitString(file_path, "/", &paths);
    std::string path;
    for (size_t i = 0; i < paths.size(); i++) {
        path += ("/" + paths[i]);
    }
    LOG(DEBUG, "Release file lock for %s",file_path.c_str());
    std::string cur_path = path;
    for (size_t i = 0; i < paths.size() ; i++) {
        UnlockInternal(cur_path);
        cur_path.resize(cur_path.find_last_of('/'));
    }
    // last unlock "/"
    UnlockInternal("/");
}

void FileLockManager::LockInternal(const std::string& path,
                                     LockType lock_type) {
    LockEntry* entry = NULL;
    {
        MutexLock lock(&mu_);
        auto it = lock_map_.find(path);
        if (it == lock_map_.end()) {
            entry = new LockEntry();
            // hold a ref for lock_map_
            entry->ref_.Inc();
            lock_map_.insert(std::make_pair(path, entry));
        } else {
            entry = it->second;
        }
        // inc ref_ first to prevent deconstruct
        entry->ref_.Inc();
    }
    // shouldn't hold mu_ here
    if (lock_type == kRead) {
        //get read lock
        entry->rw_lock_.ReadLock();
        LOG(DEBUG, "Get read lock for %s", path.c_str());
    } else {
        //get write lock
        entry->rw_lock_.WriteLock();
        LOG(DEBUG, "Get write lock for %s", path.c_str());
    }
}

void FileLockManager::UnlockInternal(const std::string& path) {
    MutexLock lock(&mu_);
    auto it = lock_map_.find(path);
    assert(it != lock_map_.end());
    LockEntry* entry = it->second;
    //release lock
    entry->rw_lock_.Unlock();
    LOG(DEBUG, "Unlock for %s", path.c_str());
    if (entry->ref_.Dec() == 1) {
        // we are the last holder
        /// TODO maybe don't need to deconstruct immediately
        delete entry;
        lock_map_.erase(it);
    }
}

FileLockManager::~FileLockManager() {

}

} // namespace bfs
} // namespace baidu

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
