// Copyright (c) 2014, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "chunkserver_manager.h"

#include <algorithm>

#include <gflags/gflags.h>
#include <common/logging.h>
#include <common/string_util.h>
#include <common/util.h>
#include "nameserver/block_mapping_manager.h"
#include "nameserver/location_provider.h"

DECLARE_int32(keepalive_timeout);
DECLARE_int32(chunkserver_max_pending_buffers);
DECLARE_int32(recover_speed);
DECLARE_int32(recover_dest_limit);
DECLARE_int32(heartbeat_interval);
DECLARE_bool(select_chunkserver_by_zone);
DECLARE_bool(select_chunkserver_by_tag);
DECLARE_double(select_chunkserver_local_factor);
DECLARE_int32(blockreport_interval);
DECLARE_int32(blockreport_size);
DECLARE_int32(expect_chunkserver_num);

namespace baidu {
namespace bfs {

int64_t Blocks::GetReportId() {
    return report_id_;
}

void Blocks::Insert(int64_t block_id) {
    MutexLock lock(&new_blocks_mu_);
    new_blocks_.insert(block_id);
}

void Blocks::Remove(int64_t block_id) {
    MutexLock blocks_lock(&block_mu_);
    blocks_.erase(block_id);

    MutexLock new_blocks_lock(&new_blocks_mu_);
    new_blocks_.erase(block_id);
}

void Blocks::CleanUp(std::set<int64_t>* blocks) {
    assert(blocks->empty());
    std::set<int64_t> tmp;
    {
        MutexLock blocks_lock(&block_mu_);
        std::swap(*blocks, blocks_);

        MutexLock new_block_lock(&new_blocks_mu_);
        std::swap(tmp, new_blocks_);
    }
    blocks->insert(tmp.begin(), tmp.end());
}

int64_t Blocks::CheckLost(int64_t report_id, std::set<int64_t>& blocks,
                  int64_t start, int64_t end, std::vector<int64_t>* lost) {
    MutexLock block_lock(&block_mu_);
    bool pass_check = true;
    for (auto it = blocks.begin(); it != blocks.end(); ++it) {
        pass_check &= blocks_.insert(*it).second;
    }
    if (pass_check) {
        LOG(DEBUG, "C%d pass block check", cs_id_);
        return report_id;
    }
    if (report_id != -1 && report_id <= report_id_) {
        LOG(INFO, "Report out-date C%d current_id %ld report_id %ld", cs_id_, report_id_, report_id);
        return report_id_;
    }

    // report_id == -1 means this is an old-version cs, skip check
    if (report_id != -1) {
        for (auto ns_it = blocks_.lower_bound(start); ns_it != blocks_.end() && *ns_it <= end;) {
            if (blocks_.find(*ns_it) == blocks_.end()) {
                LOG(WARNING, "Check Block for C%d missing #%ld ", cs_id_, *ns_it);
                lost->push_back(*ns_it);
                blocks_.erase(ns_it++);
                continue;
            }
            ns_it++;
        }
    }
    MutexLock new_blocks_lock(&new_blocks_mu_);
    std::set<int64_t> delta;
    std::swap(delta, new_blocks_);
    for (auto it = delta.begin(); it != delta.end(); ++it) {
        blocks_.insert(*it);
    }
    report_id_ = report_id;
    return report_id;
}

ChunkServerManager::ChunkServerManager(ThreadPool* thread_pool, BlockMappingManager* block_mapping_manager)
    : thread_pool_(thread_pool),
      block_mapping_manager_(block_mapping_manager),
      chunkserver_num_(0),
      next_chunkserver_id_(1) {
    memset(&stats_, 0, sizeof(stats_));
    thread_pool_->AddTask(std::bind(&ChunkServerManager::DeadCheck, this));
    thread_pool_->AddTask(std::bind(&ChunkServerManager::LogStats, this));
    localhostname_ = common::util::GetLocalHostName();
    localzone_ = LocationProvider(localhostname_, "").GetZone();
    params_.set_report_interval(FLAGS_blockreport_interval);
    params_.set_report_size(FLAGS_blockreport_size);
    params_.set_recover_size(FLAGS_recover_speed);
    params_.set_keepalive_timeout(FLAGS_keepalive_timeout);
    LOG(INFO, "Localhost: %s, localzone: %s",
        localhostname_.c_str(), localzone_.c_str());
}

void ChunkServerManager::CleanChunkServer(ChunkServerInfo* cs, const std::string& reason) {
    int32_t id = cs->id();
    MutexLock lock(&mu_, "CleanChunkServer", 10);
    chunkserver_num_--;
    auto it = block_map_.find(id);
    assert(it != block_map_.end());
    std::set<int64_t> blocks;
    it->second->CleanUp(&blocks);
    LOG(INFO, "Remove ChunkServer C%d %s %s, cs_num=%d",
            cs->id(), cs->address().c_str(), reason.c_str(), chunkserver_num_);
    cs->set_status(kCsCleaning);
    mu_.Unlock();
    block_mapping_manager_->DealWithDeadNode(id, blocks);
    mu_.Lock("CleanChunkServerRelock", 10);
    cs->set_w_qps(0);
    cs->set_w_speed(0);
    cs->set_r_qps(0);
    cs->set_r_speed(0);
    cs->set_recover_speed(0);
    if (std::find(chunkservers_to_offline_.begin(),
                  chunkservers_to_offline_.end(),
                  cs->address()) == chunkservers_to_offline_.end()) {
        if (cs->is_dead()) {
            cs->set_status(kCsOffLine);
        } else {
            cs->set_status(kCsStandby);
        }
    } else {
        cs->set_status(kCsReadonly);
    }
}

bool ChunkServerManager::KickChunkServer(int32_t cs_id) {
    MutexLock lock(&mu_, "KickChunkServer", 10);
    ChunkServerInfo* cs = NULL;
    if (!GetChunkServerPtr(cs_id, &cs)) {
        return false;
    }
    cs->set_kick(true);
    return true;
}
bool ChunkServerManager::RemoveChunkServer(const std::string& addr) {
    MutexLock lock(&mu_, "RemoveChunkServer", 10);
    std::map<std::string, int32_t>::iterator it = address_map_.find(addr);
    if (it == address_map_.end()) {
        return false;
    }
    ChunkServerInfo* cs_info = NULL;
    bool ret = GetChunkServerPtr(it->second, &cs_info);
    assert(ret);
    if (cs_info->status() == kCsActive) {
        cs_info->set_status(kCsWaitClean);
        std::function<void ()> task =
            std::bind(&ChunkServerManager::CleanChunkServer,
                        this, cs_info, std::string("Dead"));
        thread_pool_->AddTask(task);
    }
    return true;
}

void ChunkServerManager::DeadCheck() {
    int32_t now_time = common::timer::now_time();

    MutexLock lock(&mu_, "DeadCheck", 10);
    std::map<int32_t, std::set<ChunkServerInfo*> >::iterator it = heartbeat_list_.begin();

    while (it != heartbeat_list_.end()
           && it->first + params_.keepalive_timeout() <= now_time) {
        std::set<ChunkServerInfo*>::iterator node = it->second.begin();
        while (node != it->second.end()) {
            ChunkServerInfo* cs = *node;
            it->second.erase(node++);
            LOG(INFO, "[DeadCheck] ChunkServer dead C%d %s, cs_num=%d",
                cs->id(), cs->address().c_str(), chunkserver_num_);
            cs->set_is_dead(true);
            if (cs->status() == kCsActive || cs->status() == kCsReadonly) {
                cs->set_status(kCsWaitClean);
                std::function<void ()> task =
                    std::bind(&ChunkServerManager::CleanChunkServer,
                                this, cs, std::string("Dead"));
                thread_pool_->AddTask(task);
            } else {
                LOG(INFO, "[DeadCheck] ChunkServer C%d %s is being clean",
                    cs->id(), cs->address().c_str());
            }
        }
        assert(it->second.empty());
        heartbeat_list_.erase(it);
        it = heartbeat_list_.begin();
    }
    int idle_time = 5;
    if (it != heartbeat_list_.end()) {
        idle_time = it->first + params_.keepalive_timeout() - now_time;
        // LOG(INFO, "it->first= %d, now_time= %d\n", it->first, now_time);
        if (idle_time > 5) {
            idle_time = 5;
        }
    }
    thread_pool_->DelayTask(idle_time * 1000,
                           std::bind(&ChunkServerManager::DeadCheck, this));
}

bool ChunkServerManager::HandleRegister(const std::string& ip,
                                        const RegisterRequest* request,
                                        RegisterResponse* response) {
    const std::string& address = request->chunkserver_addr();
    StatusCode status = kOK;
    int cs_id = -1;
    MutexLock lock(&mu_, "HandleRegister", 10);
    std::map<std::string, int32_t>::iterator it = address_map_.find(address);
    if (it == address_map_.end()) {
        cs_id = AddChunkServer(request->chunkserver_addr(), ip,
                               request->tag(), request->disk_quota());
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
            UpdateChunkServer(cs_id, request->tag(), request->disk_quota());
            auto it = block_map_.find(cs_id);
            assert(it != block_map_.end());
            response->set_report_id(it->second->GetReportId());
            LOG(INFO, "Reconnect chunkserver C%d %s, cs_num=%d, report_id=%ld",
                cs_id, address.c_str(), chunkserver_num_, it->second->GetReportId());
        }
    }
    response->set_chunkserver_id(cs_id);
    response->set_report_interval(FLAGS_blockreport_interval);
    response->set_report_size(FLAGS_blockreport_size);
    response->set_status(status);
    return chunkserver_num_ >= FLAGS_expect_chunkserver_num;
}

void ChunkServerManager::HandleHeartBeat(const HeartBeatRequest* request, HeartBeatResponse* response) {
    int32_t id = request->chunkserver_id();
    const std::string& address = request->chunkserver_addr();
    int cs_id = GetChunkServerId(address);
    if (id == -1 || cs_id != id) {
        //reconnect after DeadCheck()
        LOG(INFO, "HandleHeartBeat unknown chunkserver %s with namespace version %ld",
            address.c_str(), request->namespace_version());
        response->set_status(kUnknownCs);
        return;
    }
    response->set_status(kOK);

    MutexLock lock(&mu_, "HandleHeartBeat", 10);
    ChunkServerInfo* info = NULL;
    bool ret = GetChunkServerPtr(id, &info);
    assert(ret && info);
    if (!info->is_dead()) {
        assert(heartbeat_list_.find(info->last_heartbeat()) != heartbeat_list_.end());
        heartbeat_list_[info->last_heartbeat()].erase(info);
        if (heartbeat_list_[info->last_heartbeat()].empty()) {
            heartbeat_list_.erase(info->last_heartbeat());
        }
    } else if (info->status() == kCsOffLine) {
        LOG(INFO, "Dead chunkserver revival C%d %s", cs_id, address.c_str());
        assert(heartbeat_list_.find(info->last_heartbeat()) == heartbeat_list_.end());
        char buf[20];
        common::timer::now_time_str(buf, 20, common::timer::kMin);
        info->set_start_time(std::string(buf));
        info->set_is_dead(false);
        info->set_status(kCsActive);
        chunkserver_num_++;
    } else {
        return;
    }
    info->set_data_size(request->data_size());
    info->set_block_num(request->block_num());
    info->set_buffers(request->buffers());
    info->set_pending_writes(request->pending_writes());
    info->set_pending_recover(request->pending_recover());
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
    } else {
        info->set_load(GetChunkServerLoad(info));
    }
    response->set_report_interval(params_.report_interval());
    response->set_report_size(params_.report_size());
}

void ChunkServerManager::ListChunkServers(::google::protobuf::RepeatedPtrField<ChunkServerInfo>* chunkservers) {
    MutexLock lock(&mu_, "ListChunkServers", 10);
    for (ServerMap::iterator it = chunkservers_.begin();
                it != chunkservers_.end(); ++it) {
        ChunkServerInfo* src = it->second;
        ChunkServerInfo* dst = chunkservers->Add();
        dst->CopyFrom(*src);
    }
}

double ChunkServerManager::GetChunkServerLoad(ChunkServerInfo* cs) {
    double max_pending = FLAGS_chunkserver_max_pending_buffers * 0.8;
    double pending_score = cs->buffers() / max_pending;
    double data_score = cs->data_size() * 1.0 / cs->disk_quota();
    int64_t space_left = cs->disk_quota() - cs->data_size();

    if (data_score > 0.95 || space_left < (5L << 30) || pending_score > 1.0) {
        return 1.0;
    }
    return (data_score * data_score + pending_score) / 2;
}

void ChunkServerManager::RandomSelect(std::vector<std::pair<double, ChunkServerInfo*> >* loads,
                                      int num) {
    std::sort(loads->begin(), loads->end());
    // Add random factor
    int scope = loads->size() - (loads->size() % num);
    for (int32_t i = num; i < scope; i++) {
        int round =  i / num + 1;
        double base_load = (*loads)[i % num].first;
        int ratio = static_cast<int>((base_load + 0.0001) * 100.0 / ((*loads)[i].first + 0.0001));
        if (rand() % 100 < (ratio / round)) {
            std::swap((*loads)[i % num], (*loads)[i]);
        }
    }
}

bool ChunkServerManager::GetChunkServerChains(int num,
                          std::vector<std::pair<int32_t,std::string> >* chains,
                          const std::string& client_address) {
    MutexLock lock(&mu_, "GetChunkServerChains", 10);
    if (num > chunkserver_num_) {
        LOG(INFO, "not enough alive chunkservers [%ld] for GetChunkServerChains [%d]\n",
            chunkserver_num_, num);
        return false;
    }
    ChunkServerInfo* local_cs = NULL;
    //first take local cs of client
    std::map<std::string, int32_t>::iterator client_it = address_map_.lower_bound(client_address);
    if (client_it != address_map_.end()) {
        std::string tmp_address(client_it->first, 0, client_it->first.find_last_of(':'));
        if (tmp_address == client_address) {
            ChunkServerInfo* cs = NULL;
            if (GetChunkServerPtr(client_it->second, &cs) &&
                !cs->is_dead() &&
                !(cs->status() == kCsReadonly)) {
                local_cs = cs;
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
            if (cs->status() == kCsReadonly) {
                LOG(INFO, "Alloc ignore Chunkserver %s: is in offline progress", cs->address().c_str());
                continue;
            }
            double load = cs->load();
            if (load <= kChunkServerLoadMax) {
                double local_factor =
                    (cs == local_cs ? FLAGS_select_chunkserver_local_factor : 0) ;
                loads.push_back(std::make_pair(load - local_factor, cs));
            } else {
                LOG(DEBUG, "Alloc ignore: ChunkServer %s data %ld/%ld buffer %d",
                    cs->address().c_str(), cs->data_size(),
                    cs->disk_quota(), cs->buffers());
            }
        }
    }
    if ((int)loads.size() < num) {
        LOG(DEBUG, "Only %ld chunkserver of %d is not over overladen, GetChunkServerChains(%d) return false",
            loads.size(), chunkserver_num_, num);
        return false;
    }
    RandomSelect(&loads, num);

    if (FLAGS_select_chunkserver_by_zone) {
        int count = SelectChunkServerByZone(num, loads, chains);
        if (count < num) {
            LOG(WARNING, "SelectChunkServerByZone(%d) return %d", num, count);
            return false;
        }
    } else {
        for (int i = 0; i < num; ++i) {
            ChunkServerInfo* cs = loads[i].second;
            chains->push_back(std::make_pair(cs->id(), cs->address()));
        }
    }
    return true;
}

bool ChunkServerManager::GetRecoverChains(const std::set<int32_t>& replica,
                                          std::vector<std::string>* chains) {
    mu_.AssertHeld();
    std::map<int32_t, std::set<ChunkServerInfo*> >::iterator it = heartbeat_list_.begin();
    std::vector<std::pair<double, ChunkServerInfo*> > loads;

    std::set<std::string> tag_set;
    if (FLAGS_select_chunkserver_by_tag) {
        for (std::set<int32_t>::const_iterator it = replica.begin();
             it != replica.end(); ++it) {
            ChunkServerInfo* rep_cs = NULL;
            if (!GetChunkServerPtr(*it, &rep_cs) || rep_cs->tag().empty()) {
                continue;
            }
            tag_set.insert(rep_cs->tag());
            ///TODO: has_remote?
        }
    }
    ChunkServerInfo* remote_cs = NULL;
    for (; it != heartbeat_list_.end(); ++it) {
        std::set<ChunkServerInfo*>& set = it->second;
        for (std::set<ChunkServerInfo*>::iterator sit = set.begin(); sit != set.end(); ++sit) {
            ChunkServerInfo* cs = *sit;
            if (replica.find(cs->id()) != replica.end()) {
                LOG(INFO, "GetRecoverChains has C%d ", cs->id());
                continue;
            } else if (FLAGS_select_chunkserver_by_tag
                       && !cs->tag().empty()
                       && !tag_set.insert(cs->tag()).second) {
                continue;
            } else if (cs->zone() != localzone_) {
                if (!remote_cs) {
                    remote_cs = cs;
                }
                LOG(DEBUG, "Remote zone server C%d ignore PickRecoverBlocks", cs->id());
                continue;
            } else if (cs->status() == kCsReadonly) {
                LOG(DEBUG, "C%d is in offline progress, igore", cs->id());
                continue;
            }
            double load = cs->load();
            if (load <= kChunkServerLoadMax) {
                loads.push_back(std::make_pair(load, cs));
            } else {
                LOG(DEBUG, "Recover alloc ignore: ChunkServer %s data %ld/%ld buffer %d",
                    cs->address().c_str(), cs->data_size(),
                    cs->disk_quota(), cs->buffers());
            }
        }
    }
    if (loads.empty()) {
        if (remote_cs) {
            double load = GetChunkServerLoad(remote_cs);
            if (load != kChunkServerLoadMax) {
                LOG(INFO, "Recover to remote zone C%d ", remote_cs->id());
                loads.push_back(std::make_pair(load, remote_cs));
            }
        }
        if (loads.empty()) {
            LOG(INFO, "Recover chain failed");
            return false;
        }
    }
    RandomSelect(&loads, FLAGS_recover_dest_limit);
    for (int i = 0; i < static_cast<int>(loads.size()) && i < FLAGS_recover_dest_limit; ++i) {
        ChunkServerInfo* cs = loads[i].second;
        chains->push_back(cs->address());
    }
    return true;
}
int ChunkServerManager::SelectChunkServerByZone(int num,
        const std::vector<std::pair<double, ChunkServerInfo*> >& loads,
        std::vector<std::pair<int32_t,std::string> >* chains) {
    std::set<std::string> tag_set;
    ChunkServerInfo* remote_server = NULL;
    for(uint32_t i = 0; i < loads.size(); i++) {
        ChunkServerInfo* cs = loads[i].second;
        if (cs->zone() != localzone_) {
            if (!remote_server) remote_server = cs;
        } else {
            const std::string& tag = cs->tag();
            if (FLAGS_select_chunkserver_by_tag && !tag.empty()) {
                if (!tag_set.insert(tag).second) {
                    LOG(DEBUG, "Ignore by tag: %s %s",
                        tag.c_str(), cs->address().c_str());
                    continue;
                }
            }
            LOG(DEBUG, "Local zone %s tag %s C%d ",
                cs->zone().c_str(), cs->tag().empty() ? "null" : cs->tag().c_str(), cs->id());
            chains->push_back(std::make_pair(cs->id(), cs->address()));
            if (static_cast<int>(chains->size()) + (remote_server ? 1 : 0) >= num) {
                break;
            }
        }
    }
    if (remote_server) {
        chains->push_back(std::make_pair(remote_server->id(), remote_server->address()));
        LOG(INFO, "Remote zone %s C%d ",
            remote_server->zone().c_str(), remote_server->id());
    }
    return chains->size();
}

bool ChunkServerManager::UpdateChunkServer(int cs_id, const std::string& tag, int64_t quota) {
    mu_.AssertHeld();
    ChunkServerInfo* info = NULL;
    if (!GetChunkServerPtr(cs_id, &info)) {
        return false;
    }
    char buf[20];
    common::timer::now_time_str(buf, 20, common::timer::kMin);
    info->set_start_time(std::string(buf));
    info->set_disk_quota(quota);
    info->set_tag(tag);
    if (info->status() != kCsReadonly) {
        info->set_status(kCsActive);
    }
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

int32_t ChunkServerManager::AddChunkServer(const std::string& address,
                                           const std::string& ip,
                                           const std::string& tag,
                                           int64_t quota) {
    mu_.AssertHeld();
    ChunkServerInfo* info = new ChunkServerInfo;
    int32_t id = next_chunkserver_id_++;
    char buf[20];
    common::timer::now_time_str(buf, 20, common::timer::kMin);
    info->set_start_time(std::string(buf));
    info->set_id(id);
    info->set_address(address);
    info->set_tag(tag);
    info->set_disk_quota(quota);
    if (std::find(chunkservers_to_offline_.begin(), chunkservers_to_offline_.end(),
                address) != chunkservers_to_offline_.end()) {
        info->set_status(kCsReadonly);
    } else {
        info->set_status(kCsActive);
    }
    info->set_kick(false);
    std::string host = address.substr(0, address.find(':'));
    LocationProvider loc(host, ip);
    info->set_zone(loc.GetZone());
    info->set_datacenter(loc.GetDataCenter());
    info->set_rack(loc.GetRack());
    LOG(INFO, "New ChunkServerInfo C%d %s %s %s %s",
        id, address.c_str(), info->zone().c_str(),
        info->datacenter().c_str(), info->rack().c_str());
    chunkservers_[id] = info;
    address_map_[address] = id;
    int32_t now_time = common::timer::now_time();
    heartbeat_list_[now_time].insert(info);
    info->set_last_heartbeat(now_time);
    ++chunkserver_num_;
    Blocks* blocks = new Blocks(id);
    block_map_.insert(std::make_pair(id, blocks));
    return id;
}

std::string ChunkServerManager::GetChunkServerAddr(int32_t id) {
    MutexLock lock(&mu_, "GetChunkServerAddr", 10);
    ChunkServerInfo* cs = NULL;
    if (GetChunkServerPtr(id, &cs) && !cs->is_dead()) {
        return cs->address();
    }
    return "";
}

int32_t ChunkServerManager::GetChunkServerId(const std::string& addr) {
    MutexLock lock(&mu_, "GetChunkServerId", 10);
    std::map<std::string, int32_t>::iterator it = address_map_.find(addr);
    if (it != address_map_.end()) {
        return it->second;
    }
    return -1;
}

void ChunkServerManager::AddBlock(int32_t id, int64_t block_id) {
    Blocks* blocks = GetBlockMap(id);
    if (!blocks) {
        LOG(WARNING, "Can't find chunkserver C%d in block_map_", id);
        return;
    }
    blocks->Insert(block_id);
}

void ChunkServerManager::SetParam(const Params& p) {
    MutexLock lock(&mu_);
    if (p.report_interval() != -1) {
        params_.set_report_interval(p.report_interval());
    }
    if (p.report_size() != -1) {
        params_.set_report_size(p.report_size());
    }
    if (p.recover_size() != -1) {
        params_.set_recover_size(p.recover_size());
    }
    if (p.keepalive_timeout() != -1) {
        params_.set_keepalive_timeout(p.keepalive_timeout());
    }
    LOG(INFO, "SetParam to report_interval = %d report_size = %d "
              "recover_size = %d keepalive_timeout = %d",
            params_.report_interval(), params_.report_size(),
            params_.recover_size(), params_.keepalive_timeout());
}

void ChunkServerManager::RemoveBlock(int32_t id, int64_t block_id) {
    Blocks* blocks = GetBlockMap(id);
    if (!blocks) {
        LOG(WARNING, "Can't find chunkserver C%d in block_map_", id);
        return;
    }
    blocks->Remove(block_id);
}

void ChunkServerManager::PickRecoverBlocks(int cs_id, RecoverVec* recover_blocks,
                                           int* hi_num, bool hi_only) {
    ChunkServerInfo* cs = NULL;
    {
        MutexLock lock(&mu_, "PickRecoverBlocks 1", 10);
        if (!GetChunkServerPtr(cs_id, &cs)) {
            return;
        }
    }
    std::vector<std::pair<int64_t, std::set<int32_t> > > blocks;
    int64_t before_pick = common::timer::get_micros();
    block_mapping_manager_->PickRecoverBlocks(cs_id, params_.recover_size() - cs->pending_recover(),
                                              &blocks, hi_num, hi_only);
    int64_t before_get_recover_chain = common::timer::get_micros();
    for (std::vector<std::pair<int64_t, std::set<int32_t> > >::iterator it = blocks.begin();
         it != blocks.end(); ++it) {
        MutexLock lock(&mu_);
        recover_blocks->push_back(std::make_pair((*it).first, std::vector<std::string>()));
        if (GetRecoverChains((*it).second, &(recover_blocks->back().second))) {
            //
        } else {
            block_mapping_manager_->ProcessRecoveredBlock(cs_id, (*it).first, kGetChunkServerError);
            recover_blocks->pop_back();
        }
    }
    int64_t after_get_recover_chain = common::timer::get_micros();
    static __thread int64_t last_warning = 0;
    if (after_get_recover_chain - before_pick > 1000 * 1000) {
        int64_t now_time = common::timer::get_micros();
        if (now_time > last_warning + 10 * 1000000) {
            last_warning = now_time;
            LOG(WARNING, "C%ld pick %d recover block use %ld micros, pick use %ld micros,"
                "get cs chains use %ld micros", cs_id, recover_blocks->size(),
                after_get_recover_chain - before_pick, before_get_recover_chain - before_pick,
                after_get_recover_chain - before_get_recover_chain);
        }
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
    int32_t overload = 0;
    {
        MutexLock lock(&mu_);
        for (ServerMap::iterator it = chunkservers_.begin(); it != chunkservers_.end(); ++it) {
            ChunkServerInfo* cs = it->second;
            w_qps += cs->w_qps();
            w_speed += cs->w_speed();
            r_qps += cs->r_qps();
            r_speed += cs->r_speed();
            recover_speed += cs->recover_speed();
            if (cs->load() > kChunkServerLoadMax) {
                ++overload;
            }
        }
    }
    stats_.w_qps = w_qps;
    stats_.w_speed = w_speed;
    stats_.r_qps = r_qps;
    stats_.r_speed = r_speed;
    stats_.recover_speed = recover_speed;
    LOG(INFO, "[LogStats] w_qps=%d w_speed=%s r_qps=%d r_speed=%s recover_speed=%s overload=%d",
               w_qps, common::HumanReadableString(w_speed).c_str(), r_qps,
               common::HumanReadableString(r_speed).c_str(),
               common::HumanReadableString(recover_speed).c_str(), overload);
    thread_pool_->DelayTask(FLAGS_heartbeat_interval * 1000,
                           std::bind(&ChunkServerManager::LogStats, this));
}

void ChunkServerManager::MarkChunkServerReadonly(const std::string& chunkserver_address) {
    mu_.AssertHeld();
    std::map<std::string, int32_t>::iterator it = address_map_.find(chunkserver_address);
    if (it == address_map_.end()) {
        LOG(WARNING, "chunkserver %s not found", chunkserver_address.c_str());
        return;
    }
    int32_t cs_id = it->second;
    ChunkServerInfo* cs_info = chunkservers_[cs_id];
    if (cs_info->status() == kCsActive) {
        cs_info->set_status(kCsReadonly);
        LOG(INFO, "Mark C%d readonly", cs_id);
    }
}

StatusCode ChunkServerManager::ShutdownChunkServer(const::google::protobuf::RepeatedPtrField<std::string>&
                                                  chunkserver_address) {
    MutexLock lock(&mu_);
    if (!chunkservers_to_offline_.empty()) {
        return kInShutdownProgress;
    }
    for (int i = 0; i < chunkserver_address.size(); i++) {
        chunkservers_to_offline_.push_back(chunkserver_address.Get(i));
        MarkChunkServerReadonly(chunkservers_to_offline_.back());
    }
    //TODO: Add kick chunkserver task
    return kOK;
}

bool ChunkServerManager::GetShutdownChunkServerStat() {
    MutexLock lock(&mu_);
    return !chunkservers_to_offline_.empty();
}

Blocks* ChunkServerManager::GetBlockMap(int32_t cs_id) {
    MutexLock lock(&mu_);
    auto it = block_map_.find(cs_id);
    if (it == block_map_.end()) {
        return NULL;
    }
    return it->second;
}

int64_t ChunkServerManager::AddBlockWithCheck(int32_t id, std::set<int64_t>& blocks,
                                  int64_t start, int64_t end, std::vector<int64_t>* lost,
                                  int64_t report_id) {
    Blocks* cs_blocks = GetBlockMap(id);
    if (!cs_blocks) {
        LOG(WARNING, "Can't find chunkserver C%d", id);
        return report_id;
    }
    return cs_blocks->CheckLost(report_id, blocks, start, end, lost);
}

} // namespace bfs
} // namespace baidu
