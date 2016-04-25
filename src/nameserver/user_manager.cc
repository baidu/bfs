// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#include "nameserver/user_manager.h"

#include <gflags/gflags.h>
#include <common/logging.h>

DECLARE_string(userdb_path);

namespace baidu {
namespace bfs {

UserManager::UserManager() : last_user_id_(-1) {
    leveldb::Options options;
    options.create_if_missing = true;
    leveldb::Status s = leveldb::DB::Open(options, FLAGS_userdb_path, &db_);
    if (!s.ok()) {
        db_ = NULL;
        LOG(FATAL, "Open leveldb fail: %s\n", s.ToString().c_str());
    }
    AddUser(0, "root", "bfs");
    AddUser(0, "share", "");
    int num = 0;
    leveldb::Iterator* it = db_->NewIterator(leveldb::ReadOptions());
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        UserInfo user_info;
        bool ret = user_info.ParseFromArray(it->value().data(), it->value().size());
        assert(ret);
        if (user_info.user_id() > last_user_id_) {
            last_user_id_ = user_info.user_id();
        }
        num ++;
    }
    LOG(INFO, "Load %d users", num);
}

UserManager::~UserManager() {
    delete db_;
}
StatusCode UserManager::AddUser(int32_t user_id,
                        const std::string& user_name,
                        const std::string& token) {
    std::string key = user_name;
    std::string value;
    UserInfo user_info;
    leveldb::Status s = db_->Get(leveldb::ReadOptions(), key, &value);
    if (user_id == 0 && s.ok()) {
        return kUserExist;
    } else if(user_id == 0) {
        user_info.set_token(token);
        user_info.set_user_id(++last_user_id_);
    }
    if (!user_info.SerializeToString(&value)) {
        LOG(WARNING, "Serialize user info to string fail");
        return kNotOK;
    }
    s = db_->Put(leveldb::WriteOptions(), key, value);
    if (!s.ok()) {
        LOG(WARNING, "Write to db fail: %s", user_name.c_str());
        return kNotOK;
    }
    return kOK;
}

void UserManager::GetUserList(std::vector<std::pair<int32_t, std::string> >* list) {

    leveldb::Iterator* it = db_->NewIterator(leveldb::ReadOptions());
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        UserInfo user_info;
        bool ret = user_info.ParseFromArray(it->value().data(), it->value().size());
        if (ret) {
            list->push_back(std::make_pair(user_info.user_id(), it->key().ToString()));
        }
    }
}

int32_t UserManager::GetUserId(const std::string& user, const std::string& token) {
    std::string key = user;
    std::string value;
    leveldb::Status s = db_->Get(leveldb::ReadOptions(), key, &value);
    if (!s.ok()) {
        LOG(DEBUG, "GetUserId fail: %s", user.c_str());
        return -1;
    }
    UserInfo user_info;
    if (!user_info.ParseFromString(value)) {
        LOG(WARNING, "Parse user info fail: %s", user.c_str());
        return -1;
    }
    if (user_info.token() != token) {
        LOG(INFO, "GetUserId wrong password: %s %s %s", user.c_str(), user_info.token().c_str(), token.c_str());
        return -1;
    }
    return user_info.user_id();
}


}
}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
