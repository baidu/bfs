// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//

#include "nameserver/file_lock.h"

#include <common/logging.h>

namespace baidu {
namespace bfs {

FileLockManager* WriteLock::file_lock_manager_ = NULL;
FileLockManager* ReadLock::file_lock_manager_ = NULL;

WriteLock::WriteLock(const std::string& file_path) {
    LOG(DEBUG, "Write lock guard for %s", file_path.c_str());
    file_path_.push_back(file_path);
    file_lock_manager_->WriteLock(file_path);
}

WriteLock::WriteLock(const std::string& file_path_a,
                                 const std::string& file_path_b) {
    LOG(DEBUG, "Write lock guard for %s and %s",
                 file_path_a.c_str(), file_path_b.c_str());
    file_path_.push_back(file_path_a);
    file_path_.push_back(file_path_b);
    file_lock_manager_->WriteLock(file_path_a, file_path_b);
}

WriteLock::~WriteLock() {
    if (file_path_.size() == 1) {
        LOG(DEBUG, "Release write lock guard for %s", file_path_[0].c_str());
        file_lock_manager_->Unlock(file_path_[0]);
    } else {
        LOG(DEBUG, "Release write lock guard for %s and %s",
                      file_path_[0].c_str(), file_path_[1].c_str());
        file_lock_manager_->Unlock(file_path_[0], file_path_[1]);
    }
}

ReadLock::ReadLock(const std::string& file_path) {
    LOG(DEBUG, "Read lock guard for %s", file_path.c_str());
    file_path_ = file_path;
    file_lock_manager_->ReadLock(file_path);
}

ReadLock::~ReadLock() {
    LOG(DEBUG, "Release read lock guard for %s", file_path_.c_str());
    file_lock_manager_->Unlock(file_path_);
}

} // namespace bfs
} // namespace baidu
