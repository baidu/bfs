// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "client_manager.h"
#include "nameserver/block_mapping.h"

#include <boost/bind.hpp>
#include <gflags/gflags.h>

#include <common/thread_pool.h>
#include <common/timer.h>
#include <common/logging.h>

DECLARE_int32(sdk_lease_time);

namespace baidu {
namespace bfs {
    ClientManager::ClientManager(BlockMapping* block_mapping) {
        block_mapping_ = block_mapping;
        thread_pool_ = new ThreadPool(1);
        thread_pool_->AddTask(boost::bind(&ClientManager::DeadCheck, this));
    }
    ClientManager::~ClientManager() {
        thread_pool_->Stop(true);
        delete thread_pool_;
        std::map<std::string, ClientInfo*>::iterator it;
        for (it = client_map_.begin(); it != client_map_.end(); ++it) {
            delete it->second;
        }
    }
    StatusCode ClientManager::HandleHeartbeat(const std::string& session_id) {
        MutexLock lock(&mu_);
        std::map<std::string, ClientInfo*>::iterator it = client_map_.find(session_id);
        if (it == client_map_.end()) {
            LOG(INFO, "Session %s expired", session_id.c_str());
            return kSessionExpired;
        }
        ClientInfo* info = it->second;
        int32_t now_time = common::timer::now_time();
        int32_t last_time = info->last_heartbeat_time;
        info->last_heartbeat_time = now_time;
        heartbeat_list_[last_time].erase(info);
        if (heartbeat_list_[last_time].empty()) {
            heartbeat_list_.erase(last_time);
        }
        heartbeat_list_[now_time].insert(info);
        return kOK;
    }
    StatusCode ClientManager::AddNewClient(const std::string& session_id) {
        MutexLock lock(&mu_);
        if (client_map_.find(session_id) != client_map_.end()) {
            LOG(WARNING, "Session %s is not expired yet", session_id.c_str());
            return kNotOK;
        }
        ClientInfo* info = new ClientInfo;
        int32_t now_time = common::timer::now_time();
        info->session_id = session_id;
        info->last_heartbeat_time = now_time;
        client_map_[session_id] = info;
        heartbeat_list_[now_time].insert(info);
        LOG(INFO, "Regist new client: %s", session_id.c_str());
        return kOK;
    }
    void ClientManager::DeadCheck() {
        int32_t now_time = common::timer::now_time();
        MutexLock lock(&mu_);
        std::map<int32_t, std::set<ClientInfo*> >::iterator it = heartbeat_list_.begin();
        while (it != heartbeat_list_.end() &&
                it->first + FLAGS_sdk_lease_time <= now_time) {
            std::set<ClientInfo*>::iterator client_it = it->second.begin();
            while (client_it != it->second.end()) {
                LOG(INFO, "Client %s lease expired", (*client_it)->session_id.c_str()); 
                std::set<int64_t>::iterator block_it = (*client_it)->writing_blocks.begin();
                for (; block_it != (*client_it)->writing_blocks.end(); ++block_it) {
                    block_mapping_->MarkIncomplete(*block_it);
                    LOG(INFO, "Close writing block #%ld ", *block_it);
                }
                client_map_.erase((*client_it)->session_id);
                delete *client_it;
                ++client_it;
            }
            heartbeat_list_.erase(it);
            it = heartbeat_list_.begin();
        }
        thread_pool_->DelayTask(6 * 1000, boost::bind(&ClientManager::DeadCheck, this));
    }
    void ClientManager::AddWritingBlock(const std::string& session_id, int64_t block_id) {
        MutexLock lock(&mu_);
        std::map<std::string, ClientInfo*>::iterator it = client_map_.find(session_id);
        if (it == client_map_.end()) {
            LOG(WARNING, "Can't find client %s", session_id.c_str());
            return;
        }
        ClientInfo* info = it->second;
        info->writing_blocks.insert(block_id);
        LOG(INFO, "Client %s is now writing block #%ld", session_id.c_str(), block_id);
    }
    void ClientManager::RemoveWritingBlock(const std::string& session_id, int64_t block_id) {
        MutexLock lock(&mu_);
        std::map<std::string, ClientInfo*>::iterator it = client_map_.find(session_id);
        if (it == client_map_.end()) {
            LOG(WARNING, "Can't find client %s", session_id.c_str());
            return;
        }
        ClientInfo* info = it->second;
        info->writing_blocks.erase(block_id);
        LOG(INFO, "Client %s finish writing block #%ld", session_id.c_str(), block_id);
    }
}
}
