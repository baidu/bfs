// Copyright (c) 2014, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#include "nameserver_impl.h"

#include <set>
#include <map>

#include <boost/bind.hpp>
#include <gflags/gflags.h>
#include <sofa/pbrpc/pbrpc.h>

#include "common/counter.h"
#include "common/logging.h"
#include "common/mutex.h"
#include "common/timer.h"
#include "common/thread_pool.h"
#include "common/string_util.h"

#include "nameserver/namespace.h"

DECLARE_int32(keepalive_timeout);
DECLARE_int32(default_replica_num);
DECLARE_int32(nameserver_safemode_time);
DECLARE_int32(chunkserver_max_pending_buffers);

namespace baidu {
namespace bfs {

common::Counter g_get_location;
common::Counter g_add_block;
common::Counter g_heart_beat;
common::Counter g_block_report;
common::Counter g_unlink;
common::Counter g_create_file;
common::Counter g_list_dir;
common::Counter g_report_blocks;

class BlockManager {
public:
    struct NSBlock {
        int64_t id;
        int64_t version;
        std::set<int32_t> replica;
        int64_t block_size;
        int32_t expect_replica_num;
        bool pending_change;
        std::set<int32_t> pulling_chunkservers;
        NSBlock(int64_t block_id)
         : id(block_id), version(-1), block_size(0),
           expect_replica_num(FLAGS_default_replica_num),
           pending_change(true) {
        }
    };
    BlockManager():_next_block_id(1) {}
    int64_t NewBlockID() {
        MutexLock lock(&_mu, "BlockManager::NewBlockID", 1000);
        return _next_block_id++;
    }
    /*
    bool RemoveReplicaBlock(int64_t block_id, int32_t chunkserver_id) {
        MutexLock lock(&_mu, "BlockManager::RemoveReplicaBlock", 1000);
        NSBlockMap::iterator it = _block_map.find(block_id);
        if (it != _block_map.end()) {
            std::set<int32_t>::iterator cs = it->second->replica.find(chunkserver_id);
            if (cs != it->second->replica.end()) {
                it->second->replica.erase(cs);
                if (it->second->replica.empty()) {
                    delete it->second;
                    _block_map.erase(it);
                }
                return true;
            } else {
                return false;
            }
        } else {
            // not report yet ?
            return false;
        }
    }*/
    bool GetBlock(int64_t block_id, NSBlock* block) {
        MutexLock lock(&_mu, "BlockManager::GetBlock", 1000);
        NSBlockMap::iterator it = _block_map.find(block_id);
        if (it == _block_map.end()) {
            return false;
        }
        if (block) {
            *block = *(it->second);
        }
        return true;
    }
    bool MarkBlockStable(int64_t block_id) {
        MutexLock lock(&_mu);
        NSBlock* nsblock = NULL;
        NSBlockMap::iterator it = _block_map.find(block_id);
        if (it != _block_map.end()) {
            nsblock = it->second;
            //assert(nsblock->pending_change == true);
            nsblock->pending_change = false;
            return true;
        } else {
            LOG(WARNING, "Can't find block: #%ld ", block_id);
            return false;
        }
    }
    bool GetReplicaLocation(int64_t id, std::set<int32_t>* chunkserver_id) {
        MutexLock lock(&_mu);
        NSBlock* nsblock = NULL;
        NSBlockMap::iterator it = _block_map.find(id);
        bool ret = false;
        if (it != _block_map.end()) {
            nsblock = it->second;
            *chunkserver_id = nsblock->replica;
            ret = true;
        } else {
            LOG(WARNING, "Can't find block: #%ld ", id);
        }

        return ret;
    }
    void DealDeadBlocks(int32_t id, std::set<int64_t> blocks) {
        LOG(INFO, "Replicate %d blocks of dead chunkserver: %d\n", blocks.size(), id);
        MutexLock lock(&_mu);
        std::set<int64_t>::iterator it = blocks.begin();
        for (; it != blocks.end(); ++it) {
            //may have been unlinked, not in _block_map
            NSBlockMap::iterator nsb_it = _block_map.find(*it);
            if (nsb_it != _block_map.end()) {
                NSBlock* nsblock = nsb_it->second;
                nsblock->replica.erase(id);
                nsblock->pulling_chunkservers.erase(id);
                if (nsblock->pulling_chunkservers.empty() &&
                        nsblock->pending_change) {
                    nsblock->pending_change = false;
                }
            }
        }
        _blocks_to_replicate.erase(id);
    }
    bool ChangeReplicaNum(int64_t block_id, int32_t replica_num) {
        MutexLock lock(&_mu);
        NSBlockMap::iterator it = _block_map.find(block_id);
        if (it == _block_map.end()) {
            assert(0);
        } else {
            NSBlock* nsblock = it->second;
            nsblock->expect_replica_num = replica_num;
            return true;
        }
    }
    void AddNewBlock(int64_t block_id) {
        MutexLock lock(&_mu);
        NSBlock* nsblock = NULL;
        NSBlockMap::iterator it = _block_map.find(block_id);
        //Don't suppport soft link now
        assert(it == _block_map.end());
        nsblock = new NSBlock(block_id);
        _block_map[block_id] = nsblock;
        LOG(DEBUG, "Init block info: #%ld ", block_id);
        if (_next_block_id <= block_id) {
            _next_block_id = block_id + 1;
        }
    }
    bool UpdateBlockInfo(int64_t id, int32_t server_id, int64_t block_size,
                         int64_t block_version, int32_t* more_replica_num = NULL) {
        MutexLock lock(&_mu);
        NSBlock* nsblock = NULL;
        NSBlockMap::iterator it = _block_map.find(id);
        if (it == _block_map.end()) {
            //have been removed
            LOG(DEBUG, "UpdateBlockInfo(%ld) has been removed", id);
            return false;
        } else {
            nsblock = it->second;
            if (nsblock->version >= 0 && block_version >= 0 &&
                    nsblock->version != block_version) {
                LOG(INFO, "block #%ld on slow chunkserver: %d,"
                        " NSB version: %ld, cs version: %ld, drop it",
                        id, server_id, nsblock->version, block_version);
                return false;
            }
            if (nsblock->block_size !=  block_size && block_size) {
                // update
                if (nsblock->block_size) {
                    LOG(WARNING, "block #%ld size mismatch", id);
                    assert(0);
                    return false;
                } else {
                    LOG(INFO, "block #%ld size update, %ld to %ld",
                        id, nsblock->block_size, block_size);
                    nsblock->block_size = block_size;
                }
            } else {
                //LOG(DEBUG, "UpdateBlockInfo(%ld) ignored, from %ld to %ld",
                //    id, nsblock->block_size, block_size);
            }
        }
        std::pair<std::set<int32_t>::iterator, bool> ret = nsblock->replica.insert(server_id);
        int32_t cur_replica_num = nsblock->replica.size();
        int32_t expect_replica_num = nsblock->expect_replica_num;
        if (cur_replica_num != expect_replica_num) {
            if (!nsblock->pending_change) {
                nsblock->pending_change = true;
                if (cur_replica_num > expect_replica_num) {
                    LOG(INFO, "too much replica cur=%d expect=%d server=%d",
                        server_id, cur_replica_num, expect_replica_num);
                    nsblock->replica.erase(ret.first);
                    return false;
                } else {
                    // add new replica
                    if (more_replica_num) {
                        *more_replica_num = expect_replica_num - cur_replica_num;
                        LOG(INFO, "Need to add %d new replica for #%ld cur=%d expect=%d",
                            *more_replica_num, id, cur_replica_num, expect_replica_num);
                    }
                }
            }
        }
        return true;
    }
    void RemoveBlocksForFile(const FileInfo& file_info) {
        for (int i = 0; i < file_info.blocks_size(); i++) {
            int64_t block_id = file_info.blocks(i);
            std::set<int32_t> chunkservers;
            GetReplicaLocation(block_id, &chunkservers);
            RemoveBlock(block_id);
            LOG(INFO, "Remove block #%ld for %s", block_id, file_info.name().c_str());
        }
    }
    void RemoveBlock(int64_t block_id) {
        MutexLock lock(&_mu);
        NSBlockMap::iterator it = _block_map.find(block_id);
        if (it == _block_map.end()) {
            LOG(WARNING, "RemoveBlock(%ld) not found", block_id);
            return;
        }
        delete it->second;
        _block_map.erase(it);
    }
    bool MarkPullBlock(int32_t dst_cs, int64_t block_id) {
        MutexLock lock(&_mu);
        NSBlockMap::iterator it = _block_map.find(block_id);
        assert(it != _block_map.end());
        bool ret = false;
        NSBlock* nsblock = it->second;
        if (nsblock->pulling_chunkservers.find(dst_cs) ==
                nsblock->pulling_chunkservers.end()) {
            nsblock->pulling_chunkservers.insert(dst_cs);
            _blocks_to_replicate[dst_cs].insert(block_id);
            LOG(INFO, "Add replicate info dst cs: %d, block #%ld",
                    dst_cs, block_id);
            ret = true;
        }
        return ret;
    }
    void UnmarkPullBlock(int32_t cs_id, int64_t block_id) {
        MutexLock lock(&_mu);
        NSBlockMap::iterator it = _block_map.find(block_id);
        if (it != _block_map.end()) {
            NSBlock* nsblock = it->second;
            assert(nsblock);
            nsblock->pulling_chunkservers.erase(cs_id);
            if (nsblock->pulling_chunkservers.empty() && nsblock->pending_change) {
                nsblock->pending_change = false;
                LOG(INFO, "Block #%ld on cs %d finish replicate\n", block_id, cs_id);
            }
            nsblock->replica.insert(cs_id);
        } else {
            LOG(WARNING, "Can't find block: #%ld ", block_id);
        }
    }
    bool GetPullBlocks(int32_t id, std::vector<std::pair<int64_t, std::set<int32_t> > >* blocks) {
        MutexLock lock(&_mu);
        bool ret = false;
        std::map<int32_t, std::set<int64_t> >::iterator it = _blocks_to_replicate.find(id);
        if (it != _blocks_to_replicate.end()) {
            std::set<int64_t>::iterator block_it = it->second.begin();
            for (; block_it != it->second.end(); ++block_it) {
                blocks->push_back(std::make_pair(*block_it, _block_map[*block_it]->replica));
            }
            _blocks_to_replicate.erase(it);
            ret = true;
        }
        return ret;
    }
    bool SetBlockVersion(int64_t block_id, int64_t version) {
        bool ret = true;
        MutexLock lock(&_mu);
        NSBlockMap::iterator it = _block_map.find(block_id);
        if (it == _block_map.end()) {
            LOG(WARNING, "Can't find block: #%ld ", block_id);
            ret = false;
        } else {
            it->second->version = version;
        }
        return ret;
    }

private:
    Mutex _mu;
    typedef std::map<int64_t, NSBlock*> NSBlockMap;
    NSBlockMap _block_map;
    int64_t _next_block_id;
    std::map<int32_t, std::set<int64_t> > _blocks_to_replicate;
};

class ChunkServerManager {
public:
    ChunkServerManager(ThreadPool* thread_pool, BlockManager* block_manager)
        : _thread_pool(thread_pool),
          _block_manager(block_manager),
          _chunkserver_num(0),
          _next_chunkserver_id(1) {
        _thread_pool->AddTask(boost::bind(&ChunkServerManager::DeadCheck, this));
    }
    void DeadCheck() {
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
                    boost::bind(&BlockManager::DealDeadBlocks,
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
    void IncChunkServerNum() {
        ++_chunkserver_num;
    }
    int32_t GetChunkServerNum() {
        return _chunkserver_num;
    }
    void HandleHeartBeat(const HeartBeatRequest* request, HeartBeatResponse* response) {
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
    void ListChunkServers(::google::protobuf::RepeatedPtrField<ChunkServerInfo>* chunkservers) {
        MutexLock lock(&_mu, "ListChunkServers", 1000);
        for (ServerMap::iterator it = _chunkservers.begin();
                    it != _chunkservers.end(); ++it) {
            ChunkServerInfo* src = it->second;
            ChunkServerInfo* dst = chunkservers->Add();
            dst->CopyFrom(*src);
        }
    }
    bool GetChunkServerChains(int num,
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
    int64_t AddChunkServer(const std::string& address, int64_t quota, int cs_id = -1) {
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
    std::string GetChunkServerAddr(int32_t id) {
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
    int32_t GetChunkserverId(const std::string& addr) {
        MutexLock lock(&_mu);
        std::map<std::string, int32_t>::iterator it = _address_map.find(addr);
        if (it != _address_map.end()) {
            return it->second;
        }
        return -1;
    }
    void AddBlock(int32_t id, int64_t block_id) {
        MutexLock lock(&_mu);
        _chunkserver_block_map[id].insert(block_id);
    }
    void RemoveBlock(int32_t id, int64_t block_id) {
        MutexLock lock(&_mu);
        _chunkserver_block_map[id].erase(block_id);
    }

private:
    ThreadPool* _thread_pool;
    BlockManager* _block_manager;
    Mutex _mu;      /// _chunkservers list mutext;
    typedef std::map<int32_t, ChunkServerInfo*> ServerMap;
    ServerMap _chunkservers;
    std::map<std::string, int32_t> _address_map;
    std::map<int32_t, std::set<ChunkServerInfo*> > _heartbeat_list;
    std::map<int32_t, std::set<int64_t> > _chunkserver_block_map;
    int32_t _chunkserver_num;
    int32_t _next_chunkserver_id;
};

NameServerImpl::NameServerImpl() : _safe_mode(true) {
    _namespace = new NameSpace();
    _block_manager = new BlockManager();
    _chunkserver_manager = new ChunkServerManager(&_thread_pool, _block_manager);
    _namespace->RebuildBlockMap(boost::bind(&NameServerImpl::RebuildBlockMapCallback, this, _1));
    _thread_pool.AddTask(boost::bind(&NameServerImpl::LogStatus, this));
    _thread_pool.DelayTask(FLAGS_nameserver_safemode_time * 1000,
        boost::bind(&NameServerImpl::LeaveSafemode, this));
}

NameServerImpl::~NameServerImpl() {
}

void NameServerImpl::LeaveSafemode() {
    LOG(INFO, "Nameserver leave safemode");
    _safe_mode = false;
}

void NameServerImpl::LogStatus() {
    LOG(INFO, "[Status] create %ld list %ld get_loc %ld add_block %ld "
              "unlink %ld report %ld %ld heartbeat %ld",
        g_create_file.Clear(), g_list_dir.Clear(), g_get_location.Clear(),
        g_add_block.Clear(), g_unlink.Clear(), g_block_report.Clear(),
        g_report_blocks.Clear(), g_heart_beat.Clear());
    _thread_pool.DelayTask(1000, boost::bind(&NameServerImpl::LogStatus, this));
}

void NameServerImpl::HeartBeat(::google::protobuf::RpcController* controller,
                         const HeartBeatRequest* request,
                         HeartBeatResponse* response,
                         ::google::protobuf::Closure* done) {
    g_heart_beat.Inc();
    // printf("Receive HeartBeat() from %s\n", request->data_server_addr().c_str());
    int64_t version = request->namespace_version();
    if (version == _namespace->Version()) {
        _chunkserver_manager->HandleHeartBeat(request, response);
    }
    response->set_namespace_version(_namespace->Version());
    done->Run();
}

void NameServerImpl::BlockReport(::google::protobuf::RpcController* controller,
                   const BlockReportRequest* request,
                   BlockReportResponse* response,
                   ::google::protobuf::Closure* done) {
    g_block_report.Inc();
    int32_t cs_id = request->chunkserver_id();
    int64_t version = request->namespace_version();
    LOG(INFO, "Report from %d, %s, %d blocks\n",
        cs_id, request->chunkserver_addr().c_str(), request->blocks_size());
    const ::google::protobuf::RepeatedPtrField<ReportBlockInfo>& blocks = request->blocks();
    response->set_namespace_version(version);
    if (version != _namespace->Version()) {
        if (blocks.size() == 0) {
            cs_id = _chunkserver_manager->AddChunkServer(request->chunkserver_addr(),
                                                         request->disk_quota());
            response->set_namespace_version(_namespace->Version());
        } else {
            // Clean it~
            for (int i = 0; i < blocks.size(); i++) {
                response->add_obsolete_blocks(blocks.Get(i).block_id());
            }
            LOG(INFO, "Unknown chunkserver namespace version %ld id= %d",
                version, cs_id);
        }
    } else {
        int old_id = _chunkserver_manager->GetChunkserverId(request->chunkserver_addr());
        if (old_id == -1) {
            if (!request->is_complete()) {
                response->set_status(403);
                done->Run();
                return;
            }
            cs_id = _chunkserver_manager->AddChunkServer(request->chunkserver_addr(),
                                                         request->disk_quota(), -1);
        } else if (cs_id == -1) {
            cs_id = old_id;
            _chunkserver_manager->IncChunkServerNum();
            LOG(INFO, "Reconnect chunkserver %d %s, cs_num=%d",
                cs_id, request->chunkserver_addr().c_str(), _chunkserver_manager->GetChunkServerNum());
        } else if (cs_id != old_id) {
            // bug...
            LOG(WARNING, "Chunkserver %s id mismatch, old: %d new: %d",
                request->chunkserver_addr().c_str(), old_id, cs_id);
            response->set_status(-1);
            done->Run();
            return;
        }
        for (int i = 0; i < blocks.size(); i++) {
            g_report_blocks.Inc();
            const ReportBlockInfo& block =  blocks.Get(i);
            int64_t cur_block_id = block.block_id();
            int64_t cur_block_size = block.block_size();

            // update block -> cs
            int32_t more_replica_num = 0;
            int64_t block_version = block.version();
            if (!_block_manager->UpdateBlockInfo(cur_block_id, cs_id,
                                                 cur_block_size,
                                                 block_version,
                                                 &more_replica_num)) {
                response->add_obsolete_blocks(cur_block_id);
                _chunkserver_manager->RemoveBlock(cs_id, cur_block_id);
                LOG(INFO, "obsolete_block: #%ld", cur_block_id);
                continue;
            }

            // update cs -> block
            _chunkserver_manager->AddBlock(cs_id, cur_block_id);
            if (!_safe_mode && more_replica_num != 0) {
                std::vector<std::pair<int32_t, std::string> > chains;
                ///TODO: Not get all chunkservers, but get more.
                if (_chunkserver_manager->GetChunkServerChains(more_replica_num, &chains)) {
                    std::set<int32_t> cur_replica_location;
                    _block_manager->GetReplicaLocation(cur_block_id, &cur_replica_location);

                    std::vector<std::pair<int32_t, std::string> >::iterator chains_it = chains.begin();
                    int num;
                    for (num = 0; num < more_replica_num &&
                            chains_it != chains.end(); ++chains_it) {
                        if (cur_replica_location.find(chains_it->first) == cur_replica_location.end()) {
                            bool mark_pull = _block_manager->MarkPullBlock(chains_it->first, cur_block_id);
                            if (mark_pull) {
                                num++;
                            }
                        }
                    }
                    //no suitable chunkserver
                    if (num == 0) {
                        _block_manager->MarkBlockStable(cur_block_id);
                    }
                }
            }
        }

        // recover replica
        std::vector<std::pair<int64_t, std::set<int32_t> > > pull_blocks;
        if (_block_manager->GetPullBlocks(cs_id, &pull_blocks)) {
            ReplicaInfo* info = NULL;
            for (size_t i = 0; i < pull_blocks.size(); i++) {
                info = response->add_new_replicas();
                info->set_block_id(pull_blocks[i].first);
                std::set<int32_t>::iterator it = pull_blocks[i].second.begin();
                for (; it != pull_blocks[i].second.end(); ++it) {
                    std::string cs_addr = _chunkserver_manager->GetChunkServerAddr(*it);
                    info->add_chunkserver_address(cs_addr);
                }
                LOG(INFO, "Add pull block: #%ld dst cs: %d", pull_blocks[i].first, cs_id);
            }
        }
    }
    response->set_chunkserver_id(cs_id);
    done->Run();
}

void NameServerImpl::PullBlockReport(::google::protobuf::RpcController* controller,
                   const PullBlockReportRequest* request,
                   PullBlockReportResponse* response,
                   ::google::protobuf::Closure* done) {
    response->set_sequence_id(request->sequence_id());
    response->set_status(0);
    int32_t chunkserver_id = request->chunkserver_id();
    for (int i = 0; i < request->blocks_size(); i++) {
        _block_manager->UnmarkPullBlock(chunkserver_id, request->blocks(i));
    }
    done->Run();
}

void NameServerImpl::CreateFile(::google::protobuf::RpcController* controller,
                        const CreateFileRequest* request,
                        CreateFileResponse* response,
                        ::google::protobuf::Closure* done) {
    g_create_file.Inc();
    response->set_sequence_id(request->sequence_id());
    const std::string& file_name = request->file_name();
    int flags = request->flags();
    int mode = request->mode();
    int status = _namespace->CreateFile(file_name, flags, mode);
    response->set_status(status);
    done->Run();
}

void NameServerImpl::AddBlock(::google::protobuf::RpcController* controller,
                         const AddBlockRequest* request,
                         AddBlockResponse* response,
                         ::google::protobuf::Closure* done) {
    g_add_block.Inc();
    response->set_sequence_id(request->sequence_id());
    const std::string& path = request->file_name();
    FileInfo file_info;
    if (!_namespace->GetFileInfo(path, &file_info)) {
        LOG(WARNING, "AddBlock file not found: %s", path.c_str());
        response->set_status(404);
        done->Run();
        return;
    }

    /// replica num
    int replica_num = file_info.replicas();
    /// check lease for write
    std::vector<std::pair<int32_t, std::string> > chains;
    if (_chunkserver_manager->GetChunkServerChains(replica_num, &chains)) {
        int64_t new_block_id = _block_manager->NewBlockID();
        LOG(INFO, "[AddBlock] new block for %s id= #%ld ",
            path.c_str(), new_block_id);
        LocatedBlock* block = response->mutable_block();
        _block_manager->AddNewBlock(new_block_id);
        for (int i =0; i<replica_num; i++) {
            ChunkServerInfo* info = block->add_chains();
            info->set_address(chains[i].second);
            LOG(INFO, "Add %s to #%ld response", chains[i].second.c_str(), new_block_id);
            _block_manager->UpdateBlockInfo(new_block_id, chains[i].first, 0, 0);
        }
        block->set_block_id(new_block_id);
        response->set_status(0);
        file_info.add_blocks(new_block_id);
        file_info.set_version(-1);
        ///TODO: Lost update? Get&Update not atomic.
        if (!_namespace->UpdateFileInfo(file_info)) {
            LOG(WARNING, "Update file info fail: %s", path.c_str());
            response->set_status(826);
        }
    } else {
        LOG(INFO, "AddBlock for %s failed.", path.c_str());
        response->set_status(886);
    }
    done->Run();
}

void NameServerImpl::FinishBlock(::google::protobuf::RpcController* controller,
                         const FinishBlockRequest* request,
                         FinishBlockResponse* response,
                         ::google::protobuf::Closure* done) {
    int64_t block_id = request->block_id();
    int64_t block_version = request->block_version();
    response->set_sequence_id(request->sequence_id());
    if (!_block_manager->SetBlockVersion(block_id, block_version)) {
        response->set_status(886);
        done->Run();
        return;
    }
    if (_block_manager->MarkBlockStable(block_id)) {
        response->set_status(0);
    } else {
        response->set_status(886);
    }
    done->Run();
}

void NameServerImpl::GetFileLocation(::google::protobuf::RpcController* controller,
                      const FileLocationRequest* request,
                      FileLocationResponse* response,
                      ::google::protobuf::Closure* done) {
    response->set_sequence_id(request->sequence_id());
    const std::string& path = request->file_name();
    LOG(INFO, "NameServerImpl::GetFileLocation: %s\n", request->file_name().c_str());
    // Get file_key
    g_get_location.Inc();

    FileInfo info;
    if (!_namespace->GetFileInfo(path, &info)) {
        // No this file
        LOG(INFO, "NameServerImpl::GetFileLocation: NotFound: %s",
            request->file_name().c_str());
        response->set_status(404);
    } else {
        for (int i=0; i<info.blocks_size(); i++) {
            int64_t block_id = info.blocks(i);
            BlockManager::NSBlock nsblock(block_id);
            if (!_block_manager->GetBlock(block_id, &nsblock)) {
                LOG(WARNING, "GetFileLocation GetBlock fail #%ld ", block_id);
                continue;
            } else {
                LocatedBlock* lcblock = response->add_blocks();
                lcblock->set_block_id(block_id);
                lcblock->set_block_size(nsblock.block_size);
                for (std::set<int32_t>::iterator it = nsblock.replica.begin();
                        it != nsblock.replica.end(); ++it) {
                    int32_t server_id = *it;
                    if (nsblock.pulling_chunkservers.find(server_id) !=
                            nsblock.pulling_chunkservers.end()) {
                        LOG(INFO, "replica is under construction #%ld on %d", block_id, server_id);
                        continue;
                    }
                    std::string addr = _chunkserver_manager->GetChunkServerAddr(server_id);
                    if (addr == "") {
                        LOG(INFO, "GetChunkServerAddr from id:%d fail.", server_id);
                        continue;
                    }
                    LOG(INFO, "return server %d %s for #%ld ", server_id, addr.c_str(), block_id);
                    ChunkServerInfo* cs_info = lcblock->add_chains();
                    cs_info->set_address(addr);
                }
            }
        }
        LOG(INFO, "NameServerImpl::GetFileLocation: %s return %d",
            request->file_name().c_str(), info.blocks_size());
        // success if file exist
        response->set_status(0);
    }
    done->Run();
}

void NameServerImpl::ListDirectory(::google::protobuf::RpcController* controller,
                        const ListDirectoryRequest* request,
                        ListDirectoryResponse* response,
                        ::google::protobuf::Closure* done) {
    g_list_dir.Inc();
    response->set_sequence_id(request->sequence_id());
    std::string path = request->path();
    common::timer::AutoTimer at(100, "ListDirectory", path.c_str());

    int status = _namespace->ListDirectory(path, response->mutable_files());
    response->set_status(status);
    done->Run();
}

void NameServerImpl::Stat(::google::protobuf::RpcController* controller,
                          const StatRequest* request,
                          StatResponse* response,
                          ::google::protobuf::Closure* done) {
    response->set_sequence_id(request->sequence_id());
    std::string path = request->path();
    LOG(INFO, "Stat: %s\n", path.c_str());

    FileInfo info;
    if (_namespace->GetFileInfo(path, &info)) {
        FileInfo* out_info = response->mutable_file_info();
        out_info->CopyFrom(info);
        int64_t file_size = 0;
        for (int i = 0; i < out_info->blocks_size(); i++) {
            int64_t block_id = out_info->blocks(i);
            BlockManager::NSBlock nsblock(block_id);
            if (!_block_manager->GetBlock(block_id, &nsblock)) {
                continue;
            }
            file_size += nsblock.block_size;
        }
        out_info->set_size(file_size);
        response->set_status(0);
        LOG(INFO, "Stat: %s return: %ld", path.c_str(), file_size);
    } else {
        LOG(WARNING, "Stat: %s return: not found", path.c_str());
        response->set_status(404);
    }
    done->Run();
}

void NameServerImpl::Rename(::google::protobuf::RpcController* controller,
                            const RenameRequest* request,
                            RenameResponse* response,
                            ::google::protobuf::Closure* done) {
    response->set_sequence_id(request->sequence_id());
    const std::string& oldpath = request->oldpath();
    const std::string& newpath = request->newpath();

    bool need_unlink;
    FileInfo remove_file;
    int status = _namespace->Rename(oldpath, newpath, &need_unlink, &remove_file);
    if (status == 0 && need_unlink) {
        _block_manager->RemoveBlocksForFile(remove_file);
    }
    response->set_status(status);
    done->Run();
}

void NameServerImpl::Unlink(::google::protobuf::RpcController* controller,
                            const UnlinkRequest* request,
                            UnlinkResponse* response,
                            ::google::protobuf::Closure* done) {
    g_unlink.Inc();
    response->set_sequence_id(request->sequence_id());
    const std::string& path = request->path();

    FileInfo file_info;
    int status = _namespace->RemoveFile(path, &file_info);
    if (status == 0) {
        _block_manager->RemoveBlocksForFile(file_info);
    }
    LOG(INFO, "Unlink: %s return %d", path.c_str(), status);
    response->set_status(status);
    done->Run();
}

void NameServerImpl::DeleteDirectory(::google::protobuf::RpcController* controller,
                                     const DeleteDirectoryRequest* request,
                                     DeleteDirectoryResponse* response,
                                     ::google::protobuf::Closure* done)  {
    response->set_sequence_id(request->sequence_id());
    std::string path = request->path();
    bool recursive = request->recursive();
    if (path.empty() || path[0] != '/') {
        response->set_status(886);
        done->Run();
    }
    std::vector<FileInfo> removed;
    int ret_status = _namespace->DeleteDirectory(path, recursive, &removed);
    for (uint32_t i = 0; i < removed.size(); i++) {
        _block_manager->RemoveBlocksForFile(removed[i]);
    }
    response->set_status(ret_status);
    done->Run();
}

void NameServerImpl::ChangeReplicaNum(::google::protobuf::RpcController* controller,
                                      const ChangeReplicaNumRequest* request,
                                      ChangeReplicaNumResponse* response,
                                      ::google::protobuf::Closure* done) {
    response->set_sequence_id(request->sequence_id());
    std::string file_name = request->file_name();
    int32_t replica_num = request->replica_num();

    int ret_status = 886;

    FileInfo file_info;
    if (_namespace->GetFileInfo(file_name, &file_info)) {
        file_info.set_replicas(replica_num);
        bool ret = _namespace->UpdateFileInfo(file_info);
        assert(ret);
        if (_block_manager->ChangeReplicaNum(file_info.entry_id(), replica_num)) {
            LOG(INFO, "Change %s replica num to %d", file_name.c_str(), replica_num);
            ret_status = 0;
        } else {
            LOG(WARNING, "Change %s replica num to %d fail", file_name.c_str(), replica_num);
        }
    } else {
        LOG(WARNING, "Change replica num not found: %s", file_name.c_str());
        ret_status = 404;
    }
    response->set_status(ret_status);
    done->Run();
}

void NameServerImpl::RebuildBlockMapCallback(const FileInfo& file_info) {
    for (int i = 0; i < file_info.blocks_size(); i++) {
        int64_t block_id = file_info.blocks(i);
        int64_t version = file_info.version();
        _block_manager->AddNewBlock(block_id);
        _block_manager->SetBlockVersion(block_id, version);
        _block_manager->ChangeReplicaNum(block_id, file_info.replicas());
        _block_manager->MarkBlockStable(block_id);
    }
}

void NameServerImpl::SysStat(::google::protobuf::RpcController* controller,
                             const SysStatRequest* request,
                             SysStatResponse* response,
                             ::google::protobuf::Closure* done) {
    sofa::pbrpc::RpcController* ctl = reinterpret_cast<sofa::pbrpc::RpcController*>(controller);
    LOG(INFO, "SysStat from %s", ctl->RemoteAddress().c_str());
    _chunkserver_manager->ListChunkServers(response->mutable_chunkservers());
    response->set_status(0);
    done->Run();
}

bool NameServerImpl::WebService(const sofa::pbrpc::HTTPRequest& request,
                                sofa::pbrpc::HTTPResponse& response) {
    ::google::protobuf::RepeatedPtrField<ChunkServerInfo>* chunkservers
        = new ::google::protobuf::RepeatedPtrField<ChunkServerInfo>;
    _chunkserver_manager->ListChunkServers(chunkservers);

    std::string table_str;
    std::string str =
            "<html><head><title>BFS console</title>\n"
            "<meta http-equiv=\"Content-Type\" content=\"text/html; charset=utf-8\" />\n"
            //"<link rel=\"stylesheet\" type=\"text/css\" "
            //    "href=\"http://www.w3school.com.cn/c5.css\"/>\n"
            //"<style> body { background: #f9f9f9;}"
            //"a:link,a:visited{color:#4078c0;} a:link{text-decoration:none;}"
            //"</style>\n"
            "<script src=\"http://libs.baidu.com/jquery/1.8.3/jquery.min.js\"></script>\n"
            "<link href=\"http://apps.bdimg.com/libs/bootstrap/3.2.0/css/bootstrap.min.css\" rel=\"stylesheet\">\n"
            "</head>\n";
    str += "<body><div class=\"col-sm-12  col-md-12\">";

    table_str +=
        "<table class=\"table\">"
        "<tr><td>id</td><td>address</td><td>blocks</td><td>Data size</td>"
        "<td>Disk quota</td><td>Disk used</td><td>Writing buffers</td>"
        "<td>alive</td><td>last_check</td><tr>";
    int dead_num = 0;
    int64_t total_quota = 0;
    int64_t total_data = 0;
    int overladen_num = 0;
    for (int i = 0; i < chunkservers->size(); i++) {
        const ChunkServerInfo& chunkserver = chunkservers->Get(i);
        if (chunkservers->Get(i).is_dead()) {
            dead_num++;
        } else {
            total_quota += chunkserver.disk_quota();
            total_data += chunkserver.data_size();
            if (chunkserver.buffers() > FLAGS_chunkserver_max_pending_buffers * 0.8) {
                overladen_num++;
            }
        }

        table_str += "</td><td>";
        table_str += common::NumToString(chunkserver.id());
        table_str += "</td><td>";
        table_str += "<a href=\"http://" + chunkserver.address() + "/dfs\">"
               + chunkserver.address() + "</a>";
        table_str += "</td><td>";
        table_str += common::NumToString(chunkserver.block_num());
        table_str += "</td><td>";
        table_str += common::HumanReadableString(chunkserver.data_size()) + "B";
        table_str += "</td><td>";
        table_str += common::HumanReadableString(chunkserver.disk_quota()) + "B";
        std::string ratio = common::NumToString(
            chunkserver.data_size() * 100 / chunkserver.disk_quota());
        table_str += "</td><td><div class=\"progress\" style=\"margin-bottom:0\">"
               "<div class=\"progress-bar\" "
                    "role=\"progressbar\" aria-valuenow=\""+ ratio + "\" aria-valuemin=\"0\" "
                    "aria-valuemax=\"100\" style=\"width: "+ ratio + "%\">" + ratio + "%"
               "</div></div>";
        table_str += "</td><td>";
        table_str += common::NumToString(chunkserver.buffers());
        table_str += "</td><td>";
        table_str += chunkserver.is_dead() ? "dead" : "alive";
        table_str += "</td><td>";
        table_str += common::NumToString(
                        common::timer::now_time() - chunkserver.last_heartbeat());
        table_str += "</td></tr>";
    }
    table_str += "</table>";

    str += "<h1>分布式文件系统控制台 - NameServer</h1>";
    str += "<h2 align=left>Nameserver status</h2>";
    str += "<p align=left>Total: " + common::HumanReadableString(total_quota) + "B</p>";
    str += "<p align=left>Used: " + common::HumanReadableString(total_data) + "B</p>";
    str += "<p align=left>Pending tasks: "
        + common::NumToString(_thread_pool.PendingNum()) + "</p>";
    str += "<p align=left>Safemode: " + common::NumToString(_safe_mode) + "</p>";
    str += "<p align=left><a href=\"/service?name=baidu.bfs.NameServer\">Rpc status</a></p>";
    str += "<h2 align=left>Chunkserver status</h2>";
    str += "<p align=left>Total: " + common::NumToString(chunkservers->size())+"</p>";
    str += "<p align=left>Alive: " + common::NumToString(chunkservers->size() - dead_num)+"</p>";
    str += "<p align=left>Dead: " + common::NumToString(dead_num)+"</p>";
    str += "<p align=left>Overload: " + common::NumToString(overladen_num)+"</p>";
    str += "<script> var int = setInterval('window.location.reload()', 1000);"
           "function check(box) {"
           "if(box.checked) {"
           "    int = setInterval('window.location.reload()', 1000);"
           "} else {"
           "    clearInterval(int);"
           "}"
           "}</script>"
           "<input onclick=\"javascript:check(this)\" "
           "checked=\"checked\" type=\"checkbox\">自动刷新</input>";
    str += table_str;
    str += "</div></body></html>";
    delete chunkservers;
    response.content = str;
    return true;
}

} // namespace bfs
} // namespace baidu

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
