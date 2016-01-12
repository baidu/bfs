// Copyright (c) 2014, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "chunckserver_manager.h"

#include <boost/bind.hpp>
#include <gflags/gflags.h>

#include <common/logging.h>

DECLARE_int32(keepalive_timeout);
DECLARE_int32(chunkserver_max_pending_buffers);

namespace baidu {
namespace bfs {

enum ChunkServerStatus {
    //kCsInit = 0,
    kCsActive = 1,
    kCsWaitClean = 2,
    kCsCleaning = 3,
    kCsOffLine = 4,
    kCsStandby = 5,
};

ChunkServerManager::ChunkServerManager(ThreadPool* thread_pool, BlockMapping* block_manager)
    : thread_pool_(thread_pool),
      block_manager_(block_manager),
      chunkserver_num_(0),
      next_chunkserver_id_(1) {
    thread_pool_->AddTask(boost::bind(&ChunkServerManager::DeadCheck, this));
}

void ChunkServerManager::CleanChunkserver(ChunkServerInfo* cs, const std::string& reason) {
    MutexLock lock(&mu_);
    chunkserver_num_--;
    LOG(INFO, "Remove Chunkserver[%d] %s %s, cs_num=%d",
        cs->id(), cs->address().c_str(), reason.c_str(), chunkserver_num_);
    int32_t id = cs->id();
    std::set<int64_t> blocks;
    std::swap(blocks, chunkserver_block_map_[id]);
    chunkserver_block_map_.erase(id);
    cs->set_status(kCsCleaning);
    mu_.Unlock();
    block_manager_->DealWithDeadBlocks(id, blocks);
    mu_.Lock();
    if (cs->is_dead()) {
        cs->set_status(kCsOffLine);
    } else {
        cs->set_status(kCsStandby);
    }
}

bool ChunkServerManager::RemoveChunkServer(const std::string& addr) {
    MutexLock lock(&mu_);
    std::map<std::string, int32_t>::iterator it = address_map_.find(addr);
    if (it == address_map_.end()) {
        return false;
    }
    int cs_id = it->second;
    ServerMap::iterator info_it = chunkservers_.find(cs_id);
    assert(info_it != chunkservers_.end());
    ChunkServerInfo* cs_info = info_it->second;
    if (cs_info->status() == kCsActive) {
        cs_info->set_status(kCsWaitClean);
        boost::function<void ()> task =
            boost::bind(&ChunkServerManager::CleanChunkserver,
                        this, cs_info, std::string("Dead"));
        thread_pool_->AddTask(task);
    }
    return true;
}

void ChunkServerManager::DeadCheck() {
    int32_t now_time = common::timer::now_time();

    MutexLock lock(&mu_);
    std::map<int32_t, std::set<ChunkServerInfo*> >::iterator it = heartbeat_list_.begin();

    while (it != heartbeat_list_.end()
           && it->first + FLAGS_keepalive_timeout <= now_time) {
        std::set<ChunkServerInfo*>::iterator node = it->second.begin();
        while (node != it->second.end()) {
            ChunkServerInfo* cs = *node;
            it->second.erase(node++);
            LOG(INFO, "[DeadCheck] Chunkserver[%d] %s dead, cs_num=%d",
                cs->id(), cs->address().c_str(), chunkserver_num_);
            cs->set_is_dead(true);
            if (cs->status() == kCsActive) {
                cs->set_status(kCsWaitClean);
                boost::function<void ()> task =
                    boost::bind(&ChunkServerManager::CleanChunkserver,
                                this, cs, std::string("Dead"));
                thread_pool_->AddTask(task);
            } else {
                LOG(INFO, "[DeadCheck] Chunkserver[%d] %s is being clean",
                    cs->id(), cs->address().c_str());
            }
        }
        assert(it->second.empty());
        heartbeat_list_.erase(it);
        it = heartbeat_list_.begin();
    }
    int idle_time = 5;
    if (it != heartbeat_list_.end()) {
        idle_time = it->first + FLAGS_keepalive_timeout - now_time;
        // LOG(INFO, "it->first= %d, now_time= %d\n", it->first, now_time);
        if (idle_time > 5) {
            idle_time = 5;
        }
    }
    thread_pool_->DelayTask(idle_time * 1000,
                           boost::bind(&ChunkServerManager::DeadCheck, this));
}

void ChunkServerManager::IncChunkServerNum() {
    ++chunkserver_num_;
}

int32_t ChunkServerManager::GetChunkServerNum() {
    return chunkserver_num_;
}

void ChunkServerManager::HandleRegister(const RegisterRequest* request,
                                        RegisterResponse* response) {
    const std::string& address = request->chunkserver_addr();
    int status = 0;
    int cs_id = -1;
    MutexLock lock(&mu_);
    std::map<std::string, int32_t>::iterator it = address_map_.find(address);
    if (it == address_map_.end()) {
        cs_id = AddChunkServer(request->chunkserver_addr(), request->disk_quota());
        assert(cs_id >= 0);
        response->set_chunkserver_id(cs_id);
    } else {
        cs_id = it->second;
        ServerMap::iterator info_it = chunkservers_.find(cs_id);
        assert(info_it != chunkservers_.end());
        ChunkServerInfo* cs_info = info_it->second;
        if (cs_info->status() == kCsWaitClean || cs_info->status() == kCsCleaning) {
            status = -1;
            LOG(INFO, "Reconnect chunkserver %d %s, cs_num=%d, internal cleaning",
                cs_id, address.c_str(), chunkserver_num_);
        } else {
            UpdateChunkServer(cs_id, request->disk_quota());
            LOG(INFO, "Reconnect chunkserver %d %s, cs_num=%d",
                cs_id, address.c_str(), chunkserver_num_);
        }
    }
    response->set_chunkserver_id(cs_id);
    response->set_status(status);
}

void ChunkServerManager::HandleHeartBeat(const HeartBeatRequest* request, HeartBeatResponse* response) {
    MutexLock lock(&mu_);
    int32_t id = request->chunkserver_id();
    ServerMap::iterator it = chunkservers_.find(id);
    ChunkServerInfo* info = NULL;
    if (it != chunkservers_.end()) {
        info = it->second;
        assert(info);
        if (!info->is_dead()) {
            assert(heartbeat_list_.find(info->last_heartbeat()) != heartbeat_list_.end());
            heartbeat_list_[info->last_heartbeat()].erase(info);
            if (heartbeat_list_[info->last_heartbeat()].empty()) {
                heartbeat_list_.erase(info->last_heartbeat());
            }
        } else {
            assert(heartbeat_list_.find(info->last_heartbeat()) == heartbeat_list_.end());
            info->set_is_dead(false);
        }
    } else {
        //reconnect after DeadCheck()
        LOG(WARNING, "Unknown chunkserver %d with namespace version %ld",
            id, request->namespace_version());
        response->set_status(-2);
        return;
        /*
        info = new ChunkServerInfo;
        info->set_id(id);
        info->set_address(request->data_server_addr());
        LOG(INFO, "New ChunkServerInfo[%id] %p ", id, info);
        chunkservers_[id] = info;
        ++_chunkserver_num;*/
    }
    info->set_data_size(request->data_size());
    info->set_block_num(request->block_num());
    info->set_buffers(request->buffers());
    int32_t now_time = common::timer::now_time();
    heartbeat_list_[now_time].insert(info);
    info->set_last_heartbeat(now_time);
}

void ChunkServerManager::ListChunkServers(::google::protobuf::RepeatedPtrField<ChunkServerInfo>* chunkservers) {
    MutexLock lock(&mu_, "ListChunkServers", 1000);
    for (ServerMap::iterator it = chunkservers_.begin();
                it != chunkservers_.end(); ++it) {
        ChunkServerInfo* src = it->second;
        ChunkServerInfo* dst = chunkservers->Add();
        dst->CopyFrom(*src);
    }
}

bool ChunkServerManager::GetChunkServerChains(int num,
                          std::vector<std::pair<int32_t,std::string> >* chains) {
    MutexLock lock(&mu_);
    if (num > chunkserver_num_) {
        LOG(WARNING, "not enough alive chunkservers [%ld] for GetChunkServerChains [%d]\n",
            chunkserver_num_, num);
        return false;
    }
    std::map<int32_t, std::set<ChunkServerInfo*> >::iterator it = heartbeat_list_.begin();
    std::vector<std::pair<int64_t, ChunkServerInfo*> > loads;

    for (; it != heartbeat_list_.end(); ++it) {
        std::set<ChunkServerInfo*>& set = it->second;
        for (std::set<ChunkServerInfo*>::iterator sit = set.begin();
             sit != set.end(); ++sit) {
            ChunkServerInfo* cs = *sit;
            if (cs->data_size() < cs->disk_quota()
                && cs->buffers() < FLAGS_chunkserver_max_pending_buffers * 0.8) {
                loads.push_back(
                    std::make_pair(cs->data_size(), cs));
            } else {
                LOG(INFO, "Alloc ignore: Chunkserver %s data %ld/%ld buffer %d",
                    cs->address().c_str(), cs->data_size(),
                    cs->disk_quota(), cs->buffers());
            }
        }
    }
    if ((int)loads.size() < num) {
        LOG(WARNING, "Only %ld chunkserver of %ld is not over overladen, GetChunkServerChains(%d) rturne false",
            loads.size(), chunkserver_num_, num);
        return false;
    }
    std::sort(loads.begin(), loads.end());
    // Add random factor
    int scope = loads.size() - (loads.size() % num);
    for (int32_t i = num; i < scope; i++) {
        int round =  i / num + 1;
        int64_t base_load = loads[i % num].first;
        int ratio = (base_load + 1024) * 100 / (loads[i].first + 1024);
        if (rand() % 100 < (ratio / round)) {
            std::swap(loads[i % num], loads[i]);
        }
    }

    for (int i = 0; i < num; ++i) {
        ChunkServerInfo* cs = loads[i].second;
        chains->push_back(std::make_pair(cs->id(), cs->address()));
    }
    return true;
}

bool ChunkServerManager::UpdateChunkServer(int cs_id, int64_t quota) {
    mu_.AssertHeld();
    ServerMap::iterator it = chunkservers_.find(cs_id);
    if (it == chunkservers_.end()) {
        return false;
    }
    ChunkServerInfo* info = it->second;
    info->set_disk_quota(quota);
    info->set_status(kCsActive);
    if (info->is_dead()) {
        int32_t now_time = common::timer::now_time();
        heartbeat_list_[now_time].insert(info);
        info->set_last_heartbeat(now_time);
        info->set_is_dead(false);
        chunkserver_num_ ++;
    }
    return true;
}

int32_t ChunkServerManager::AddChunkServer(const std::string& address, int64_t quota) {
    mu_.AssertHeld();
    ChunkServerInfo* info = new ChunkServerInfo;
    int32_t id = next_chunkserver_id_++;
    info->set_id(id);
    info->set_address(address);
    info->set_disk_quota(quota);
    info->set_status(kCsActive);
    LOG(INFO, "New ChunkServerInfo[%d] %p", id, info);
    chunkservers_[id] = info;
    address_map_[address] = id;
    int32_t now_time = common::timer::now_time();
    heartbeat_list_[now_time].insert(info);
    info->set_last_heartbeat(now_time);
    ++chunkserver_num_;
    return id;
}

std::string ChunkServerManager::GetChunkServerAddr(int32_t id) {
    MutexLock lock(&mu_);
    ServerMap::iterator it = chunkservers_.find(id);
    if (it != chunkservers_.end()) {
        ChunkServerInfo* info = it->second;
        if (!info->is_dead()) {
            return info->address();
        }
    }
    return "";
}

int32_t ChunkServerManager::GetChunkserverId(const std::string& addr) {
    MutexLock lock(&mu_);
    std::map<std::string, int32_t>::iterator it = address_map_.find(addr);
    if (it != address_map_.end()) {
        return it->second;
    }
    return -1;
}

void ChunkServerManager::AddBlock(int32_t id, int64_t block_id) {
    MutexLock lock(&mu_);
    chunkserver_block_map_[id].insert(block_id);
}

void ChunkServerManager::RemoveBlock(int32_t id, int64_t block_id) {
    MutexLock lock(&mu_);
    chunkserver_block_map_[id].erase(block_id);
}

void ChunkServerManager::PickRecoverBlocks(int64_t cs_id, int64_t block_num,
                                           std::map<int64_t, std::string>* recover_blocks) {
    std::map<int64_t, int64_t> blocks;
    MutexLock lock(&mu_);
    block_manager_->PickRecoverBlocks(cs_id, block_num, &blocks);
    for (std::map<int64_t, int64_t>::iterator it = blocks.begin(); it != blocks.end(); ++it) {
        ServerMap::iterator cs_it = chunkservers_.find(it->second);
        if (cs_it == chunkservers_.end()) {
            LOG(INFO, "can't find chunkserver %ld", it->second);
            continue;
        }
        ChunkServerInfo* cs = cs_it->second;
        recover_blocks->insert(std::make_pair<int64_t, std::string>(it->first, cs->address()));
    }
}

} // namespace bfs
} // namespace baidu
