// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#ifndef  BFS_USER_MANAGER_H_
#define  BFS_USER_MANAGER_H_

#include <map>
#include <string>
#include <vector>
#include <leveldb/db.h>

#include "proto/user.pb.h"
#include "proto/status_code.pb.h"

namespace baidu {
namespace bfs {

class UserManager {
public:
    UserManager();
    ~UserManager();
    /// User management
    int32_t GetUserId(const std::string& user, const std::string& token);
    void GetUserList(std::vector<std::pair<int32_t, std::string> >* list);
    StatusCode AddUser(const std::string& user_name, const std::string& token);
    StatusCode DeleteUser(const std::string& user_name);
    StatusCode ChangeToken(const std::string& user_name, const std::string& old_token,
                           const std::string& new_token);
private:
    leveldb::DB* db_;
    int32_t last_user_id_;
};

}
}

#endif  // BFS_USER_MANAGER_H_

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
