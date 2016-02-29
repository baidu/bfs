// Copyright (c) 2014, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "chunkserver_manager.h"

#include <boost/bind.hpp>
#include <gflags/gflags.h>

#include <common/logging.h>
#include <common/string_util.h>
#include "proto/status_code.pb.h"
#include "nameserver/block_mapping.h"

DECLARE_int32(keepalive_timeout);
DECLARE_int32(chunkserver_max_pending_buffers);
DECLARE_int32(recover_speed);
DECLARE_int32(heartbeat_interval);

namespace baidu {
namespace bfs {

const int kChunkserverLoadMax = -1;

ChunkServerManager::ChunkServerManager(ThreadPool* thread_pool, BlockMapping* block_mapping)
    : thread_pool_(thread_pool),
      block_mapping_(block_mapping),
      chunkserver_num_(0),
      next_chunkserver_id_(1) {
    thread_pool_->AddTask(boost::bind(&ChunkServerManager::DeadCheck, this));
    thread_pool_->AddTask(boost::bind(&ChunkServerManager::LogStats, this));
}

void ChunkServerManager::CleanChunkserver(ChunkServerInfo* cs, const std::string& reason) {
    int32_t id = cs->id();
    std::set<int64_t> blocks;
    {
        MutexLock lock(&mu_);
        chunkserver_num_--;
        LOG(INFO, "Remove Chunkserver C%d %s %s, cs_num=%d",
                cs->id(), cs->address().c_str(), reason.c_str(), chunkserver_num_);
        std::swap(blocks, chunkserver_block_map_[id]);
        chunkserver_block_map_.erase(id);
        cs->set_status(kCsCleaning);
        cs->set_w_qps(0);
        cs->set_w_speed(0);
        cs->set_r_qps(0);
        cs->set_r_speed(0);
        cs->set_recover_speed(0);
        if (cs->is_dead()) {
            cs->set_status(kCsOffLine);
        } else {
            cs->set_status(kCsStandby);
        }
    }
    block_mapping_->DealWithDeadNode(id, blocks);
}

bool ChunkServerManager::KickChunkserver(int32_t cs_id) {
    MutexLock lock(&mu_);
    ChunkServerInfo* cs = NULL;
    if (!GetChunkServerPtr(cs_id, &cs)) {
        return false;
    }
    cs->set_kick(true);
    return true;
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
        LOG(INFO, "HandleHeartBeat unknown chunkserver %s with namespace version %ld",
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
        LOG(INFO, "Dead chunkserver revival C%d %s", cs_id, address.c_str());
        assert(heartbeat_list_.find(info->last_heartbeat()) == heartbeat_list_.end());
        info->set_is_dead(false);
    }
    info->set_data_size(request->data_size());
    info->set_block_num(request->block_num());
    info->set_buffers(request->buffers());
    info->set_pending_writes(request->pending_writes());
    info->set_w_qps(request->w_qps());
    info->set_w_speed(request->w_speed());
    info->set_r_qps(request->r_qps());
    info->set_r_speed(request->r_speed());
    info->set_recover_speed(request->recover_speed());
    int32_t now_time = common::timer::now_time();
    heartbeat_list_[now_time].insert(info);
    info->set_last_heartbeat(now_time);
    if (info->kick()) {
        response->set_kick(true);
    }
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

double ChunkServerManager::GetChunkserverLoad(ChunkServerInfo* cs) {
    double max_pending = FLAGS_chunkserver_max_pending_buffers * 0.8;
    double pending_socre = cs->pending_writes() / max_pending;
    double data_socre = cs->data_size() * 1.0 / cs->disk_quota();
    int64_t space_left = cs->disk_quota() - cs->data_size();

    if (data_socre > 0.95 || space_left < (5L << 30) || pending_socre > 1.0) {
        return kChunkserverLoadMax;
    }
    return data_socre * data_socre + pending_socre * pending_socre;
}

bool ChunkServerManager::GetChunkServerChains(int num,
                          std::vector<std::pair<int32_t,std::string> >* chains,
                          const std::string& client_address) {
    MutexLock lock(&mu_);
    if (num > chunkserver_num_) {
        LOG(INFO, "not enough alive chunkservers [%ld] for GetChunkServerChains [%d]\n",
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
            if (GetChunkServerPtr(client_it->second, &cs)
                && GetChunkserverLoad(cs) != kChunkserverLoadMax) {
                chains->push_back(std::make_pair(cs->id(), cs->address()));
                if (--num == 0) {
                    return true;
                }
            }
        }
    }
    std::map<int32_t, std::set<ChunkServerInfo*> >::iterator it = heartbeat_list_.begin();
    std::vector<std::pair<double, ChunkServerInfo*> > loads;

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
            double load = GetChunkserverLoad(cs);
            if (load != kChunkserverLoadMax) {
                loads.push_back(std::make_pair(load, cs));
            } else {
                LOG(INFO, "Alloc ignore: Chunkserver %s data %ld/%ld buffer %d",
                    cs->address().c_str(), cs->data_size(),
                    cs->disk_quota(), cs->buffers());
            }
        }
    }
    if ((int)loads.size() < num) {
        LOG(WARNING, "Only %ld chunkserver of %ld is not over overladen, GetChunkServerChains(%d) return false",
            loads.size(), chunkserver_num_, num);
        return false;
    }
    std::sort(loads.begin(), loads.end());
    // Add random factor
    int scope = loads.size() - (loads.size() % num);
    for (int32_t i = num; i < scope; i++) {
        int round =  i / num + 1;
        double base_load = loads[i % num].first;
        int ratio = static_cast<int>((base_load + 0.0001) * 100.0 / (loads[i].first + 0.0001));
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
    info->set_kick(false);
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
    info->set_kick(false);
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
                                           std::map<int64_t, std::string>* recover_blocks,
                                           int* hi_num) {
    {
        MutexLock lock(&mu_);
        ChunkServerInfo* cs = NULL;
        if (!GetChunkServerPtr(cs_id, &cs)) {
            return;
        }
        if (cs->buffers() > FLAGS_chunkserver_max_pending_buffers * 0.5
            || cs->data_size() > cs->disk_quota() * 0.95) {
            return;
        }
    }
    std::map<int64_t, int32_t> blocks;
    block_mapping_->PickRecoverBlocks(cs_id, FLAGS_recover_speed, &blocks, hi_num);
    for (std::map<int64_t, int32_t>::iterator it = blocks.begin(); it != blocks.end(); ++it) {
        MutexLock lock(&mu_);
        ChunkServerInfo* cs = NULL;
        if (!GetChunkServerPtr(it->second, &cs)) {
            LOG(WARNING, "PickRecoverBlocks for C%d can't find chunkserver C%d",
                cs_id, it->second);
            continue;
        }
        recover_blocks->insert(std::make_pair(it->first, cs->address()));
    }
}

void ChunkServerManager::GetStat(int32_t* w_qps, int64_t* w_speed,
                                 int32_t* r_qps, int64_t* r_speed, int64_t* recover_speed) {
    if (w_qps) *w_qps = stats_.w_qps;
    if (w_speed) *w_speed = stats_.w_speed;
    if (r_qps) *r_qps = stats_.r_qps;
    if (r_speed) *r_speed = stats_.r_speed;
    if (recover_speed) *recover_speed = stats_.recover_speed;
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

void ChunkServerManager::LogStats() {
    int32_t w_qps = 0, r_qps = 0;
    int64_t w_speed = 0, r_speed = 0, recover_speed = 0;
    for (ServerMap::iterator it = chunkservers_.begin(); it != chunkservers_.end(); ++it) {
        ChunkServerInfo* cs = it->second;
        w_qps += cs->w_qps();
        w_speed += cs->w_speed();
        r_qps += cs->r_qps();
        r_speed += cs->r_speed();
        recover_speed += cs->recover_speed();
    }
    stats_.w_qps = w_qps;
    stats_.w_speed = w_speed;
    stats_.r_qps = r_qps;
    stats_.r_speed = r_speed;
    stats_.recover_speed = recover_speed;
    LOG(INFO, "[LogStats] w_qps=%d w_speed=%s r_qps=%d r_speed=%s recover_speed=%s",
               w_qps, common::HumanReadableString(w_speed).c_str(), r_qps,
               common::HumanReadableString(r_speed).c_str(),
               common::HumanReadableString(recover_speed).c_str());
    thread_pool_->DelayTask(FLAGS_heartbeat_interval * 1000,
                           boost::bind(&ChunkServerManager::LogStats, this));
}

} // namespace bfs
} // namespace baidu
