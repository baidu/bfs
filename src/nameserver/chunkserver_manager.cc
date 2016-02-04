// Copyright (c) 2014, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "chunkserver_manager.h"

#include <boost/bind.hpp>
#include <gflags/gflags.h>

#include <common/logging.h>
#include "proto/status_code.pb.h"
#include "nameserver/block_mapping.h"

DECLARE_int32(keepalive_timeout);
DECLARE_int32(chunkserver_max_pending_buffers);
DECLARE_int32(recover_speed);

namespace baidu {
namespace bfs {

ChunkServerManager::ChunkServerManager(ThreadPool* thread_pool, BlockMapping* block_mapping)
    : thread_pool_(thread_pool),
      block_mapping_(block_mapping),
      chunkserver_num_(0),
      next_chunkserver_id_(1) {
    thread_pool_->AddTask(boost::bind(&ChunkServerManager::DeadCheck, this));
}

void ChunkServerManager::CleanChunkserver(ChunkServerInfo* cs, const std::string& reason) {
    MutexLock lock(&mu_);
    chunkserver_num_--;
    LOG(INFO, "Remove Chunkserver C%d %s %s, cs_num=%d",
        cs->id(), cs->address().c_str(), reason.c_str(), chunkserver_num_);
    int32_t id = cs->id();
    std::set<int64_t> blocks;
    std::swap(blocks, chunkserver_block_map_[id]);
    chunkserver_block_map_.erase(id);
    cs->set_status(kCsCleaning);
    mu_.Unlock();
    block_mapping_->DealWithDeadBlocks(id, blocks);
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
    ChunkServerInfo* cs_info = NULL;
    bool ret = GetChunkServerPtr(it->second, &cs_info);
    assert(ret);
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
            LOG(INFO, "[DeadCheck] Chunkserver dead C%d %s, cs_num=%d",
                cs->id(), cs->address().c_str(), chunkserver_num_);
            cs->set_is_dead(true);
            if (cs->status() == kCsActive) {
                cs->set_status(kCsWaitClean);
                boost::function<void ()> task =
                    boost::bind(&ChunkServerManager::CleanChunkserver,
                                this, cs, std::string("Dead"));
                thread_pool_->AddTask(task);
            } else {
                LOG(INFO, "[DeadCheck] Chunkserver C%d %s is being clean",
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
    StatusCode status = kOK;
    int cs_id = -1;
    MutexLock lock(&mu_);
    std::map<std::string, int32_t>::iterator it = address_map_.find(address);
    if (it == address_map_.end()) {
        cs_id = AddChunkServer(request->chunkserver_addr(), request->disk_quota());
        assert(cs_id >= 0);
        response->set_chunkserver_id(cs_id);
    } else {
        cs_id = it->second;
        ChunkServerInfo* cs_info;
        bool ret = GetChunkServerPtr(cs_id, &cs_info);
        assert(ret);
        if (cs_info->status() == kCsWaitClean || cs_info->status() == kCsCleaning) {
            status = kNotOK;
            LOG(INFO, "Reconnect chunkserver C%d %s, cs_num=%d, internal cleaning",
                cs_id, address.c_str(), chunkserver_num_);
        } else {
            UpdateChunkServer(cs_id, request->disk_quota());
            LOG(INFO, "Reconnect chunkserver C%d %s, cs_num=%d",
                cs_id, address.c_str(), chunkserver_num_);
        }
    }
    response->set_chunkserver_id(cs_id);
    response->set_status(status);
}

void ChunkServerManager::HandleHeartBeat(const HeartBeatRequest* request, HeartBeatResponse* response) {
    int32_t id = request->chunkserver_id();
    const std::string& address = request->chunkserver_addr();
    int cs_id = GetChunkserverId(address);
    if (id == -1 || cs_id != id) {
        //reconnect after DeadCheck()
        LOG(WARNING, "Unknown chunkserver %s with namespace version %ld",
            address.c_str(), request->namespace_version());
        response->set_status(kUnknownCs);
        return;
    }
    response->set_status(kOK);

    MutexLock lock(&mu_);
    ChunkServerInfo* info = NULL;
    bool ret = GetChunkServerPtr(id, &info);
    assert(ret && info);
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
                          std::vector<std::pair<int32_t,std::string> >* chains,
                          const std::string& client_address) {
    MutexLock lock(&mu_);
    if (num > chunkserver_num_) {
        LOG(WARNING, "not enough alive chunkservers [%ld] for GetChunkServerChains [%d]\n",
            chunkserver_num_, num);
        return false;
    }
    //first take local cs of client
    std::map<std::string, int32_t>::iterator client_it = address_map_.lower_bound(client_address);
    if (client_it != address_map_.end()) {
        std::string tmp_address(client_it->first, 0, client_it->first.find_last_of(':'));
        if (tmp_address == client_address &&
            heartbeat_list_.find(client_it->second) != heartbeat_list_.end()) {
            ChunkServerInfo* cs = NULL;
            if (GetChunkServerPtr(client_it->second, &cs)) {
                if (cs->data_size() < cs->disk_quota()
                        && cs->buffers() < FLAGS_chunkserver_max_pending_buffers * 0.8) {
                    chains->push_back(std::make_pair(cs->id(), cs->address()));
                    if (--num == 0) {
                        return true;
                    }
                }
            }
        }
    }
    std::map<int32_t, std::set<ChunkServerInfo*> >::iterator it = heartbeat_list_.begin();
    std::vector<std::pair<int64_t, ChunkServerInfo*> > loads;

    for (; it != heartbeat_list_.end(); ++it) {
        std::set<ChunkServerInfo*>& set = it->second;
        for (std::set<ChunkServerInfo*>::iterator sit = set.begin();
             sit != set.end(); ++sit) {
            ChunkServerInfo* cs = *sit;
            if (!chains->empty() && cs->id() == (*(chains->begin())).first) {
                // we have selected this chunkserver as it's local for this client,
                // skip it.
                continue;
            }
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
    ChunkServerInfo* info = NULL;
    if (!GetChunkServerPtr(cs_id, &info)) {
        return false;
    }
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
    LOG(INFO, "New ChunkServerInfo C%d %s %p", id, address.c_str(), info);
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
    ChunkServerInfo* cs = NULL;
    if (GetChunkServerPtr(id, &cs) && !cs->is_dead()) {
        return cs->address();
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

void ChunkServerManager::PickRecoverBlocks(int cs_id,
                                           std::map<int64_t, std::string>* recover_blocks) {
    MutexLock lock(&mu_);
    ChunkServerInfo* cs = NULL;
    if (!GetChunkServerPtr(cs_id, &cs)) {
        return;
    }
    if (cs->buffers() > FLAGS_chunkserver_max_pending_buffers * 0.5) {
        return;
    }
    std::map<int64_t, int32_t> blocks;
    block_mapping_->PickRecoverBlocks(cs_id, FLAGS_recover_speed, &blocks);
    for (std::map<int64_t, int32_t>::iterator it = blocks.begin(); it != blocks.end(); ++it) {
        ChunkServerInfo* cs = NULL;
        if (!GetChunkServerPtr(it->second, &cs)) {
            LOG(WARNING, "PickRecoverBlocks for C%d can't find chunkserver C%d",
                cs_id, it->second);
            continue;
        }
        recover_blocks->insert(std::make_pair(it->first, cs->address()));
    }
    LOG(INFO, "C%d picked %lu blocks to recover", cs_id, recover_blocks->size());
}

bool ChunkServerManager::GetChunkServerPtr(int32_t cs_id, ChunkServerInfo** cs) {
    mu_.AssertHeld();
    ServerMap::iterator it = chunkservers_.find(cs_id);
    if (it == chunkservers_.end()) {
        return false;
    }
    if (cs) *cs = it->second;
    return true;
}

} // namespace bfs
} // namespace baidu
