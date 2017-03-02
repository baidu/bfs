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
    file_path_.push_back(file_path);
    file_lock_manager_->WriteLock(file_path);
}

WriteLock::WriteLock(const std::string& file_path_a,
                     const std::string& file_path_b) {
    int r = strcmp(file_path_a.c_str(), file_path_b.c_str());
    if (r == 0) {
        file_path_.push_back(file_path_a);
        file_lock_manager_->WriteLock(file_path_a);
    } else if (r < 0) {
        file_path_.push_back(file_path_a);
        file_path_.push_back(file_path_b);
        file_lock_manager_->WriteLock(file_path_a);
        file_lock_manager_->WriteLock(file_path_b);
    } else {
        file_path_.push_back(file_path_b);
        file_path_.push_back(file_path_a);
        file_lock_manager_->WriteLock(file_path_b);
        file_lock_manager_->WriteLock(file_path_a);
    }
}

WriteLock::~WriteLock() {
    if (file_path_.size() == 1) {
        file_lock_manager_->Unlock(file_path_[0]);
    } else {
        file_lock_manager_->Unlock(file_path_[1]);
        file_lock_manager_->Unlock(file_path_[0]);
    }
}

void WriteLock::SetFileLockManager(FileLockManager* file_lock_manager) {
    file_lock_manager_ = file_lock_manager;
}

ReadLock::ReadLock(const std::string& file_path) {
    file_path_ = file_path;
    file_lock_manager_->ReadLock(file_path);
}

ReadLock::~ReadLock() {
    file_lock_manager_->Unlock(file_path_);
}

void ReadLock::SetFileLockManager(FileLockManager* file_lock_manager) {
    file_lock_manager_ = file_lock_manager;
}

} // namespace bfs
} // namespace baidu
