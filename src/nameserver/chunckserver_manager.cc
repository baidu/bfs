// Copyright (c) 2014, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "chunckserver_manager.h"

#include <boost/bind.hpp>
#include <gflags/gflags.h>

#include "common/logging.h"

DECLARE_int32(keepalive_timeout);
DECLARE_int32(chunkserver_max_pending_buffers);

namespace baidu {
namespace bfs {

ChunkServerManager::ChunkServerManager(ThreadPool* thread_pool, BlockMapping* block_manager)
    : _thread_pool(thread_pool),
      _block_manager(block_manager),
      _chunkserver_num(0),
      _next_chunkserver_id(1) {
    _thread_pool->AddTask(boost::bind(&ChunkServerManager::DeadCheck, this));
}

void ChunkServerManager::DeadCheck() {
    int32_t now_time = common::timer::now_time();

    MutexLock lock(&_mu);
    std::map<int32_t, std::set<ChunkServerInfo*> >::iterator it = _heartbeat_list.begin();

    while (it != _heartbeat_list.end()
           && it->first + FLAGS_keepalive_timeout <= now_time) {
        std::set<ChunkServerInfo*>::iterator node = it->second.begin();
        while (node != it->second.end()) {
            ChunkServerInfo* cs = *node;
            cs->set_is_dead(true);
            it->second.erase(node);
            _chunkserver_num--;
            LOG(INFO, "[DeadCheck] Chunkserver[%d] %s dead, cs_num=%d",
                cs->id(), cs->address().c_str(), _chunkserver_num);
            node = it->second.begin();

            int32_t id = cs->id();
            std::set<int64_t> blocks = _chunkserver_block_map[id];
            boost::function<void ()> task =
                boost::bind(&BlockMapping::DealDeadBlocks,
                        _block_manager, id, blocks);
            _thread_pool->AddTask(task);
            _chunkserver_block_map.erase(id);
        }
        assert(it->second.empty());
        _heartbeat_list.erase(it);
        it = _heartbeat_list.begin();
    }
    int idle_time = 5;
    if (it != _heartbeat_list.end()) {
        idle_time = it->first + FLAGS_keepalive_timeout - now_time;
        // LOG(INFO, "it->first= %d, now_time= %d\n", it->first, now_time);
        if (idle_time > 5) {
            idle_time = 5;
        }
    }
    _thread_pool->DelayTask(idle_time * 1000,
                           boost::bind(&ChunkServerManager::DeadCheck, this));
}

void ChunkServerManager::IncChunkServerNum() {
    ++_chunkserver_num;
}

int32_t ChunkServerManager::GetChunkServerNum() {
    return _chunkserver_num;
}

void ChunkServerManager::HandleHeartBeat(const HeartBeatRequest* request, HeartBeatResponse* response) {
    MutexLock lock(&_mu);
    int32_t id = request->chunkserver_id();
    ServerMap::iterator it = _chunkservers.find(id);
    ChunkServerInfo* info = NULL;
    if (it != _chunkservers.end()) {
        info = it->second;
        assert(info);
        if (!info->is_dead()) {
            assert(_heartbeat_list.find(info->last_heartbeat()) != _heartbeat_list.end());
            _heartbeat_list[info->last_heartbeat()].erase(info);
            if (_heartbeat_list[info->last_heartbeat()].empty()) {
                _heartbeat_list.erase(info->last_heartbeat());
            }
        } else {
            assert(_heartbeat_list.find(info->last_heartbeat()) == _heartbeat_list.end());
            info->set_is_dead(false);
        }
    } else {
        //reconnect after DeadCheck()
        LOG(WARNING, "Unknown chunkserver %d with namespace version %ld",
            id, request->namespace_version());
        return;
        /*
        info = new ChunkServerInfo;
        info->set_id(id);
        info->set_address(request->data_server_addr());
        LOG(INFO, "New ChunkServerInfo[%id] %p ", id, info);
        _chunkservers[id] = info;
        ++_chunkserver_num;*/
    }
    info->set_data_size(request->data_size());
    info->set_block_num(request->block_num());
    info->set_buffers(request->buffers());
    int32_t now_time = common::timer::now_time();
    _heartbeat_list[now_time].insert(info);
    info->set_last_heartbeat(now_time);
}

void ChunkServerManager::ListChunkServers(::google::protobuf::RepeatedPtrField<ChunkServerInfo>* chunkservers) {
    MutexLock lock(&_mu, "ListChunkServers", 1000);
    for (ServerMap::iterator it = _chunkservers.begin();
                it != _chunkservers.end(); ++it) {
        ChunkServerInfo* src = it->second;
        ChunkServerInfo* dst = chunkservers->Add();
        dst->CopyFrom(*src);
    }
}

bool ChunkServerManager::GetChunkServerChains(int num,
                          std::vector<std::pair<int32_t,std::string> >* chains) {
    MutexLock lock(&_mu);
    if (num > _chunkserver_num) {
        LOG(WARNING, "not enough alive chunkservers [%ld] for GetChunkServerChains [%d]\n",
            _chunkserver_num, num);
        return false;
    }
    std::map<int32_t, std::set<ChunkServerInfo*> >::iterator it = _heartbeat_list.begin();
    std::vector<std::pair<int64_t, ChunkServerInfo*> > loads;

    for (; it != _heartbeat_list.end(); ++it) {
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
            loads.size(), _chunkserver_num, num);
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

int64_t ChunkServerManager::AddChunkServer(const std::string& address, int64_t quota, int cs_id) {
    ChunkServerInfo* info = new ChunkServerInfo;
    MutexLock lock(&_mu);
    int32_t id = cs_id==-1 ? _next_chunkserver_id++ : cs_id;
    info->set_id(id);
    info->set_address(address);
    info->set_disk_quota(quota);
    LOG(INFO, "New ChunkServerInfo[%d] %p", id, info);
    _chunkservers[id] = info;
    _address_map[address] = id;
    int32_t now_time = common::timer::now_time();
    _heartbeat_list[now_time].insert(info);
    info->set_last_heartbeat(now_time);
    ++_chunkserver_num;
    return id;
}

std::string ChunkServerManager::GetChunkServerAddr(int32_t id) {
    MutexLock lock(&_mu);
    ServerMap::iterator it = _chunkservers.find(id);
    if (it != _chunkservers.end()) {
        ChunkServerInfo* info = it->second;
        if (!info->is_dead()) {
            return info->address();
        }
    }
    return "";
}

int32_t ChunkServerManager::GetChunkserverId(const std::string& addr) {
    MutexLock lock(&_mu);
    std::map<std::string, int32_t>::iterator it = _address_map.find(addr);
    if (it != _address_map.end()) {
        return it->second;
    }
    return -1;
}

void ChunkServerManager::AddBlock(int32_t id, int64_t block_id) {
    MutexLock lock(&_mu);
    _chunkserver_block_map[id].insert(block_id);
}

void ChunkServerManager::RemoveBlock(int32_t id, int64_t block_id) {
    MutexLock lock(&_mu);
    _chunkserver_block_map[id].erase(block_id);
}

} // namespace bfs
} // namespace baidu
