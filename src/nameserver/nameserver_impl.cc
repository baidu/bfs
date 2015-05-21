// Copyright (c) 2014, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#include "nameserver_impl.h"

#include <fcntl.h>
#include <set>
#include <map>

#include <boost/bind.hpp>
#include <gflags/gflags.h>
#include <leveldb/db.h>
#include <leveldb/cache.h>
#include <leveldb/write_batch.h>
#include <sofa/pbrpc/pbrpc.h>

#include "common/logging.h"
#include "common/mutex.h"
#include "common/timer.h"
#include "common/thread_pool.h"
#include "common/util.h"

DECLARE_string(namedb_path);
DECLARE_int64(namedb_cache_size);
DECLARE_string(blockdb_path);
DECLARE_int64(blockdb_cache_size);
DECLARE_int32(keepalive_timeout);
DECLARE_int32(default_replica_num);

namespace bfs {

const uint32_t MAX_PATH_LENGHT = 10240;
const uint32_t MAX_PATH_DEPTH = 99;

/// 构造标准化路径
/// /home/work/file -> 00,01/home,02/home/work,03/home/work/file
bool SplitPath(const std::string& path, std::vector<std::string>* element) {
    if (path.empty() || path[0] != '/' || path.size() > MAX_PATH_LENGHT) {
        return false;
    }
    int keylen = 2;
    char keybuf[MAX_PATH_LENGHT];
    uint32_t path_depth = 0;
    int last_pos = 0;
    bool valid = true;
    for (size_t i = 0; i <= path.size(); i++) {
        if (i == path.size() || path[i] == '/') {
            if (valid) {
                if (path_depth > MAX_PATH_DEPTH) {
                    return false;
                }
                keybuf[0] = '0' + (path_depth / 10);
                keybuf[1] = '0' + (path_depth % 10);
                memcpy(keybuf + keylen, path.data() + last_pos, i - last_pos);
                keylen += i - last_pos;
                element->push_back(std::string(keybuf, keylen));
                ++path_depth;
            }
            last_pos = i;
            valid = false;
        } else {
            valid = true;
        }
    }
#if 0
    printf("SplitPath return: ");
    for (uint32_t i=0; i < element->size(); i++) {
        printf("\"%s\",", (*element)[i].c_str());
    }
    printf("\n");
#endif
    return true;
}

class BlockManager {
public:
    struct NSBlock {
        int64_t id;
        int64_t version;
        std::set<int32_t> replica;
        int64_t block_size;
        int32_t expect_replica_num;
        bool pending_change;
        NSBlock(int64_t block_id)
         : id(block_id), version(0), expect_replica_num(FLAGS_default_replica_num),
           pending_change(true) {
        }
    };
    BlockManager():_next_block_id(1) {}
    int64_t NewBlock() {
        MutexLock lock(&_mu);
        return ++_next_block_id;
    }
    bool AddBlock(int64_t id, int32_t server_id, int64_t block_size,
                  int32_t* more_replica_num = NULL) {
        MutexLock lock(&_mu);
        NSBlock* nsblock = NULL;
        NSBlockMap::iterator it = _block_map.find(id);
        if (it == _block_map.end()) {
            nsblock = new NSBlock(id);
            _block_map[id] = nsblock;
            nsblock->block_size = block_size;
            LOG(DEBUG, "[BMAddBlock] New block %ld, size: %ld", id, block_size);
        } else {
            nsblock = it->second;
            if (nsblock->block_size !=  block_size && block_size) {
                if (nsblock->block_size) {
                    LOG(WARNING, "block size mismatch, block: %ld\n", id);
                    assert(0);
                    return false;
                } else {
                    LOG(DEBUG, "Block[%ld] size update, %ld to %ld",
                        id, nsblock->block_size, block_size);
                    nsblock->block_size = block_size;
                }
            }
        }
        if (_next_block_id <= id) {
            _next_block_id = id + 1;
        }
        /// 增加一个副本, 无论之前已经有几个了, 多余的通过gc处理
        nsblock->replica.insert(server_id);
        int32_t cur_replica_num = nsblock->replica.size();
        int32_t expect_replica_num = nsblock->expect_replica_num;
        if (cur_replica_num != expect_replica_num) {
            if (!nsblock->pending_change) {
                nsblock->pending_change = true;
                if (cur_replica_num > expect_replica_num) {
                    int32_t reduant_num = cur_replica_num - expect_replica_num;
                    //TODO select chunkservers according to some strategies
                    std::set<int32_t>::iterator nsit = nsblock->replica.begin();
                    for (int i = 0; i < reduant_num; i++, ++nsit) {
                        _obsolete_blocks[id].insert(*nsit);
                    }
                } else {
                    // add new replica
                    if (more_replica_num) {
                        *more_replica_num = expect_replica_num - cur_replica_num;
                        LOG(INFO, "Need to add %d new replica for block: %ld\n", *more_replica_num, id);
                    }
                }
            }
        }
        return true;
    }
    bool RemoveBlock(int64_t block_id, int32_t chunkserver_id) {
        MutexLock lock(&_mu);
        NSBlockMap::iterator it = _block_map.find(block_id);
        if (it != _block_map.end()) {
            std::set<int32_t>::iterator cs = it->second->replica.find(chunkserver_id);
            if (cs != it->second->replica.end()) {
                it->second->replica.erase(cs);
                return true;
            } else {
                return false;
            }
        } else {
            // not report yet ?
            return false;
        }
    }
    bool GetBlock(int64_t block_id, NSBlock* block) {
        MutexLock lock(&_mu);
        NSBlockMap::iterator it = _block_map.find(block_id);
        if (it == _block_map.end()) {
            return false;
        }
        *block = *(it->second);
        return true;
    }
    bool CheckObsoleteBlock(int64_t block_id, int32_t chunkserver_id) {
        MutexLock lock(&_mu);
        std::map<int64_t, std::set<int32_t> >::iterator it
            = _obsolete_blocks.find(block_id);
        if (it == _obsolete_blocks.end()) {
            return false;
        } else {
            std::set<int32_t>::iterator cs = it->second.find(chunkserver_id);
            if (cs != it->second.end()) {
                return true;
            } else {
                return false;
            }
        }
    }
    void MarkObsoleteBlock(int64_t block_id, int32_t chunkserver_id) {
        MutexLock lock(&_mu);
        _obsolete_blocks[block_id].insert(chunkserver_id);
    }
    void UnmarkObsoleteBlock(int64_t block_id, int32_t chunkserver_id) {
        MutexLock lock(&_mu);
        std::map<int64_t, std::set<int32_t> >::iterator it = _obsolete_blocks.find(block_id);
        if (it != _obsolete_blocks.end()) {
            std::set<int32_t>::iterator cs = it->second.find(chunkserver_id);
            if (cs != it->second.end()) {
                it->second.erase(cs);
                if (it->second.empty()) {
                    // pending_change needs to change here
                    _block_map[block_id]->pending_change = false;
                    _obsolete_blocks.erase(it);
                }
            } else {
                LOG(WARNING, "Block %ld on chunkserver %d is not marked obsolete\n",
                        block_id, chunkserver_id);
            }
        } else {
            LOG(WARNING, "Block %d is not marked obsolete\n");
        }
    }
    bool MarkFinishBlock(int64_t block_id) {
        MutexLock lock(&_mu);
        NSBlock* nsblock = NULL;
        NSBlockMap::iterator it = _block_map.find(block_id);
        if (it != _block_map.end()) {
            nsblock = it->second;
            assert(nsblock->pending_change == true);
            nsblock->pending_change = false;
            return true;
        } else {
            LOG(WARNING, "Can't find block: %ld\n", block_id);
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
            LOG(WARNING, "Can't find block: %ld\n", id);
        }

        return ret;
    }
    void ReplicateDeadBlocks(int32_t id, std::set<int64_t> blocks) {
        LOG(INFO, "Replicate %d blocks of dead chunkserver: %d\n", blocks.size(), id);
        MutexLock lock(&_mu);
        std::set<int64_t>::iterator it = blocks.begin();
        for (; it != blocks.end(); ++it) {
            NSBlockMap::iterator nsb_it = _block_map.find(*it);
            assert(nsb_it != _block_map.end());
            NSBlock* nsblock = nsb_it->second;
            nsblock->replica.erase(id);
        }
    }
    bool ChangeReplicaNum(int64_t block_id, int32_t replica_num) {
        MutexLock lock(&_mu);
        NSBlockMap::iterator it = _block_map.find(block_id);
        bool ret = false;
        if (it == _block_map.end()) {
            //maybe not report yet
        } else {
            NSBlock* nsblock = it->second;
            nsblock->expect_replica_num = replica_num;
            ret = true;
        }
        return ret;
    }
private:
    Mutex _mu;
    typedef std::map<int64_t, NSBlock*> NSBlockMap;
    NSBlockMap _block_map;
    int64_t _next_block_id;
    std::map<int64_t, std::set<int32_t> > _obsolete_blocks;
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
                LOG(INFO, "[DeadCheck] Chunkserver %s dead", cs->address().c_str());
                ///TODO: handle chunkserver fail
                int32_t id = cs->id();
                std::set<int64_t> blocks = _chunkserver_block_map[id];
                boost::function<void ()> task =
                    boost::bind(&BlockManager::ReplicateDeadBlocks,
                            _block_manager, id, blocks);
                _thread_pool->AddTask(task);
                _chunkserver_block_map.erase(id);

                it->second.erase(node);
                _chunkserver_num--;
                node = it->second.begin();
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

    void HandleHeartBeat(const HeartBeatRequest* request, HeartBeatResponse* response) {
        MutexLock lock(&_mu);
        int32_t id = request->chunkserver_id();
        ServerMap::iterator it = _chunkservers.find(id);
        ChunkServerInfo* info = NULL;
        if (it != _chunkservers.end()) {
           info = it->second;
            assert(info);
            _heartbeat_list[info->last_heartbeat()].erase(info);
            if (_heartbeat_list[info->last_heartbeat()].empty()) {
                _heartbeat_list.erase(info->last_heartbeat());
            }
        } else {
            //reconnect after DeadCheck()
            info = new ChunkServerInfo;
            info->set_id(id);
            info->set_address(request->data_server_addr());
            _chunkservers[id] = info;
            ++_chunkserver_num;
        }

        int32_t now_time = common::timer::now_time();
        _heartbeat_list[now_time].insert(info);
        info->set_last_heartbeat(now_time);
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
        std::vector<std::pair<int64_t, ChunkServerInfo*> > chunkserver_load;

        for (; it != _heartbeat_list.end(); ++it) {
            std::set<ChunkServerInfo*>& set = it->second;
            for (std::set<ChunkServerInfo*>::iterator sit = set.begin();
                 sit != set.end(); ++sit) {
                ChunkServerInfo* cs = *sit;
                chunkserver_load.push_back(
                    std::make_pair(cs->data_size(), cs));
            }
        }
        std::sort(chunkserver_load.begin(), chunkserver_load.end());

        std::vector<std::pair<int64_t, ChunkServerInfo*> >::iterator load_it;
        load_it = chunkserver_load.begin();
        for (int i = 0; i < num; ++i, ++load_it) {
            ChunkServerInfo* cs = load_it->second;
            chains->push_back(std::make_pair(cs->id(), cs->address()));
        }
        return true;
    }
    int64_t AddChunkServer(const std::string& address) {
        MutexLock lock(&_mu);
        int32_t id = _next_chunkserver_id++;
        ChunkServerInfo* info = new ChunkServerInfo;
        info->set_id(id);
        info->set_address(address);
        _chunkservers[id] = info;
        int32_t now_time = common::timer::now_time();
        _heartbeat_list[now_time].insert(info);
        info->set_last_heartbeat(now_time);
        ++_chunkserver_num;
        return id;
    }
    std::string GetChunkServer(int32_t id) {
        MutexLock lock(&_mu);
        ServerMap::iterator it = _chunkservers.find(id);
        if (it == _chunkservers.end()) {
            return "";
        } else {
            return it->second->address();
        }
    }
    bool SetChunkServerLoad(int32_t id, int64_t size) {
        MutexLock lock(&_mu);
        ServerMap::iterator it = _chunkservers.find(id);
        if(it == _chunkservers.end()) {
            LOG(WARNING, "ChunkServer does not exist!, chunkserver id: %d\n", id);
            assert(0);
            return false;
        } else {
            it->second->set_data_size(size);
            LOG(INFO, "Get Report of ChunkServerLoad, server id: %d, load: %ld\n", id, size);
            return true;
        }
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
    std::map<int32_t, std::set<ChunkServerInfo*> > _heartbeat_list;
    std::map<int32_t, std::set<int64_t> > _chunkserver_block_map;
    int32_t _chunkserver_num;
    int32_t _next_chunkserver_id;
};

NameServerImpl::NameServerImpl() {
    leveldb::Options options;
    options.create_if_missing = true;
    options.block_cache = leveldb::NewLRUCache(FLAGS_namedb_cache_size*1024L*1024L);
    leveldb::Status s = leveldb::DB::Open(options, FLAGS_namedb_path, &_name_db);
    if (!s.ok()) {
        _name_db = NULL;
        LOG(FATAL, "Open _name_db fail: %s\n", s.ToString().c_str());
    }
    options.block_cache = leveldb::NewLRUCache(FLAGS_blockdb_cache_size * 1024L * 1024L);
    s = leveldb::DB::Open(options, FLAGS_blockdb_path, &_block_db);
    if (!s.ok()) {
        _block_db = NULL;
        LOG(FATAL, "Open _block_db fail: %s\n", s.ToString().c_str());
    }
    _namespace_version = common::timer::get_micros();
    _block_manager = new BlockManager();
    _chunkserver_manager = new ChunkServerManager(&_thread_pool, _block_manager);
}
NameServerImpl::~NameServerImpl() {
}

void NameServerImpl::HeartBeat(::google::protobuf::RpcController* controller,
                         const HeartBeatRequest* request,
                         HeartBeatResponse* response,
                         ::google::protobuf::Closure* done) {
    // printf("Receive HeartBeat() from %s\n", request->data_server_addr().c_str());
    int32_t id = request->chunkserver_id();
    int64_t version = request->namespace_version();

    if (version != _namespace_version) {
        id = _chunkserver_manager->AddChunkServer(request->data_server_addr());
    } else {
        _chunkserver_manager->HandleHeartBeat(request, response);
    }
    response->set_chunkserver_id(id);
    response->set_namespace_version(_namespace_version);
    done->Run();
}

void NameServerImpl::BlockReport(::google::protobuf::RpcController* controller,
                   const BlockReportRequest* request,
                   BlockReportResponse* response,
                   ::google::protobuf::Closure* done) {
    int32_t id = request->chunkserver_id();
    int64_t version = request->namespace_version();
    LOG(INFO, "Report from %d, %s, %d blocks\n",
        id, request->chunkserver_addr().c_str(), request->blocks_size());
    if (version != _namespace_version) {
        response->set_status(8882);
    } else {
        const ::google::protobuf::RepeatedPtrField<ReportBlockInfo>& blocks = request->blocks();
        int64_t size = 0;
        for (int i = 0; i < blocks.size(); i++) {
            const ReportBlockInfo& block =  blocks.Get(i);
            int64_t cur_block_id = block.block_id();
            int64_t cur_block_size = block.block_size();

            if (_block_manager->CheckObsoleteBlock(cur_block_id, id)) {
                //add to response
                response->add_obsolete_blocks(cur_block_id);
                _block_manager->RemoveBlock(cur_block_id, id);
                _chunkserver_manager->RemoveBlock(id, cur_block_id);
                _block_manager->UnmarkObsoleteBlock(cur_block_id, id);
            } else {
                size += cur_block_size;

                int32_t more_replica_num = 0;
                _block_manager->AddBlock(cur_block_id, id, cur_block_size, &more_replica_num);
                _chunkserver_manager->AddBlock(id, cur_block_id);
                if (more_replica_num != 0) {
                    std::vector<std::pair<int32_t, std::string> > chains;
                    ///TODO: Not get all chunkservers, but get more.
                    if (_chunkserver_manager->GetChunkServerChains(more_replica_num, &chains)) {
                        std::set<int32_t> cur_replica_location;
                        _block_manager->GetReplicaLocation(cur_block_id, &cur_replica_location);

                        std::vector<std::pair<int32_t, std::string> >::iterator chains_it = chains.begin();
                        ReplicaInfo* info = NULL;
                        int num;
                        for (num = 0; num < more_replica_num &&
                                chains_it != chains.end(); ++chains_it) {
                            if (cur_replica_location.find(chains_it->first) == cur_replica_location.end()) {
                                if (num == 0) {
                                    info = response->add_new_replicas();
                                    info->set_block_id(cur_block_id);
                                }
                                LOG(INFO, "Add new replica to chunkserver %ld\n", chains_it->first);
                                info->add_chunkserver_address(chains_it->second);
                                num++;
                            }
                        }
                        //no suitable chunkserver
                        if (num == 0) {
                            _block_manager->MarkFinishBlock(cur_block_id);
                        }
                    }
                }
            }
        }
        _chunkserver_manager->SetChunkServerLoad(id, size);
    }
    done->Run();
}

void NameServerImpl::CreateFile(::google::protobuf::RpcController* controller,
                        const CreateFileRequest* request,
                        CreateFileResponse* response,
                        ::google::protobuf::Closure* done) {
    response->set_sequence_id(request->sequence_id());
    const std::string& file_name = request->file_name();
    int flags = request->flags();
    std::vector<std::string> file_keys;
    if (!SplitPath(file_name, &file_keys)) {
        response->set_status(886);
        done->Run();
        return;
    }

    /// Find parent directory, create if not exist.
    FileInfo file_info;
    std::string info_value;
    int depth = file_keys.size();
    leveldb::Status s;
    for (int i=0; i < depth-1; ++i) {
        s = _name_db->Get(leveldb::ReadOptions(), file_keys[i], &info_value);
        if (s.IsNotFound()) {
            file_info.set_type((1<<9)|0755);
            file_info.set_ctime(time(NULL));
            file_info.SerializeToString(&info_value);
            s = _name_db->Put(leveldb::WriteOptions(), file_keys[i], info_value);
            assert (s.ok());
            LOG(INFO, "Create path recursively: %s\n",file_keys[i].c_str()+2);
        } else {
            bool ret = file_info.ParseFromString(info_value);
            assert(ret);
            if ((file_info.type() & (1<<9)) == 0) {
                LOG(WARNING, "Create path fail: %s is not a directory\n", file_keys[i].c_str() + 2);
                response->set_status(886);
                done->Run();
                return;
            }
        }
    }
    
    const std::string& file_key = file_keys[depth-1];
    if ((flags & O_TRUNC) == 0) {
        s = _name_db->Get(leveldb::ReadOptions(), file_key, &info_value);
        if (s.ok()) {
            LOG(WARNING, "CreateFile %s fail: already exist!\n", file_name.c_str());
            response->set_status(1);
            done->Run();
            return;
        }
    }
    int mode = request->mode();
    if (mode) {
        file_info.set_type(((1 << 10) - 1) & mode);
    } else {
        file_info.set_type(755);
    }
    file_info.set_id(0);
    file_info.set_ctime(time(NULL));
    //file_info.add_blocks();
    file_info.SerializeToString(&info_value);
    s = _name_db->Put(leveldb::WriteOptions(), file_key, info_value);
    if (s.ok()) {
        LOG(INFO, "CreateFile %s\n", file_key.c_str());
        response->set_status(0);
    } else {
        LOG(WARNING, "CreateFile %s\n fail: db put fail", file_key.c_str());
        response->set_status(2);
    }
    done->Run();
}

void NameServerImpl::AddBlock(::google::protobuf::RpcController* controller,
                         const AddBlockRequest* request,
                         AddBlockResponse* response,
                         ::google::protobuf::Closure* done) {
    response->set_sequence_id(request->sequence_id());
    const std::string path = request->file_name();
    std::vector<std::string> elements;
    if (!SplitPath(path, &elements)) {
        LOG(WARNING, "AddBlock bad path: %s\n", path.c_str());
        response->set_status(22445);
        done->Run();
    }
    const std::string& file_key = elements[elements.size()-1];
    MutexLock lock(&_mu);
    std::string infobuf;
    leveldb::Status s = _name_db->Get(leveldb::ReadOptions(), file_key, &infobuf);
    if (!s.ok()) {
        LOG(WARNING, "AddBlock file not found: %s\n", path.c_str());
        response->set_status(2445);
        done->Run();        
    }
    
    FileInfo file_info;
    if (!file_info.ParseFromString(infobuf)) {
        assert(0);
    }
    /// replica num
    int replica_num = FLAGS_default_replica_num;
    /// check lease for write
    std::vector<std::pair<int32_t, std::string> > chains;
    if (_chunkserver_manager->GetChunkServerChains(replica_num, &chains)) {
        int64_t new_block_id = _block_manager->NewBlock();
        LOG(DEBUG, "[AddBlock] new block for %s id= %ld.",
            path.c_str(), new_block_id);
        LocatedBlock* block = response->mutable_block();
        for (int i =0; i<replica_num; i++) {
            ChunkServerInfo* info = block->add_chains();
            info->set_address(chains[i].second);
            LOG(INFO, "Add %s to response\n", chains[i].second.c_str());
            _block_manager->AddBlock(new_block_id, chains[i].first, 0);
        }
        block->set_block_id(new_block_id);
        response->set_status(0);
        file_info.add_blocks(new_block_id);
        file_info.SerializeToString(&infobuf);
        s = _name_db->Put(leveldb::WriteOptions(), file_key, infobuf);
        assert(s.ok());
        char idstr[64];
        snprintf(idstr, sizeof(idstr), "%13ld", new_block_id);
        s = _block_db->Put(leveldb::WriteOptions(), idstr, path);
        assert(s.ok());
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
    response->set_sequence_id(request->sequence_id());
    if (_block_manager->MarkFinishBlock(block_id)) {
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
    std::vector<std::string> elements;
    if (!SplitPath(path, &elements)) {
        LOG(WARNING, "GetFileLocation bad path: %s\n", path.c_str());
        response->set_status(22445);
        done->Run();
        return;
    }
    const std::string& file_key = elements[elements.size()-1];
    // Get FileInfo
    std::string infobuf;
    leveldb::Status s = _name_db->Get(leveldb::ReadOptions(), file_key, &infobuf);
    if (!s.ok()) {
        // No this file
        LOG(INFO, "NameServerImpl::GetFileLocation: NotFound: %s\n", request->file_name().c_str());
        response->set_status(110);
    } else {
        FileInfo info;
        bool ret = info.ParseFromString(infobuf);
        assert(ret);        
        for (int i=0; i<info.blocks_size(); i++) {
            int64_t block_id = info.blocks(i);
            BlockManager::NSBlock nsblock(block_id);
            if (!_block_manager->GetBlock(block_id, &nsblock)) {
                // 新加的Block, 信息还没汇报上来, 忽略它
                continue;
            } else {
                LocatedBlock* lcblock = response->add_blocks();
                lcblock->set_block_id(block_id);
                lcblock->set_block_size(nsblock.block_size);
                for (std::set<int32_t>::iterator it = nsblock.replica.begin();
                        it != nsblock.replica.end(); ++it) {
                    int32_t server_id = *it;
                    std::string addr = _chunkserver_manager->GetChunkServer(server_id);
                    LOG(INFO, "return server %s for %ld", addr.c_str(), block_id);
                    ChunkServerInfo* info = lcblock->add_chains();
                    info->set_address(addr);
                }
            }
        }
        // 找到文件了, 就返回成功
        response->set_status(0);
    }
    done->Run();
}

void NameServerImpl::ListDirectory(::google::protobuf::RpcController* controller,
                        const ListDirectoryRequest* request,
                        ListDirectoryResponse* response,
                        ::google::protobuf::Closure* done) {
    response->set_sequence_id(request->sequence_id());
    std::string path = request->path();
    std::vector<std::string> keys;
    if (path.empty() || path[0] != '/') {
        path = "/";
    }
    if (path[path.size()-1] != '/') {
        path += '/';
    }
    ///TODO: Check path existent

    path += "#";
    common::timer::AutoTimer at(100, "ListDirectory", path.c_str());
    if (!SplitPath(path, &keys)) {
        LOG(WARNING, "SplitPath fail: %s\n", path.c_str());
        response->set_status(886);
        done->Run();
        return;
    }

    const std::string& file_start_key = keys[keys.size()-1];
    std::string file_end_key = file_start_key;
    if (file_end_key[file_end_key.size()-1] == '#') {
        file_end_key[file_end_key.size()-1] = '\255';
    } else {
        file_end_key += "#";
    }

    common::timer::AutoTimer at1(100, "ListDirectory iterate", path.c_str());
    //printf("List Directory: %s, return: ", file_start_key.c_str());
    leveldb::Iterator* it = _name_db->NewIterator(leveldb::ReadOptions());
    for (it->Seek(file_start_key); it->Valid(); it->Next()) {
        leveldb::Slice key = it->key();
        if (key.compare(file_end_key)>=0) {
            break;
        }
        FileInfo* file_info = response->add_files();
        bool ret = file_info->ParseFromArray(it->value().data(), it->value().size());
        assert(ret);
        file_info->set_name(key.data()+2, it->key().size()-2);
        //printf("%s, ", file_info->name().c_str());
    }
    //printf("\n");
    delete it;
    response->set_status(0);
    
    common::timer::AutoTimer at2(100, "ListDirectory done run", path.c_str());
    done->Run();
}
void NameServerImpl::Stat(::google::protobuf::RpcController* controller,
                          const ::bfs::StatRequest* request,
                          ::bfs::StatResponse* response,
                          ::google::protobuf::Closure* done) {
    response->set_sequence_id(request->sequence_id());
    std::string path = request->path();
    LOG(INFO, "Stat: %s\n", path.c_str());

    std::vector<std::string> keys;
    if (!SplitPath(path, &keys)) {
        LOG(WARNING, "Stata SplitPath fail: %s\n", path.c_str());
        response->set_status(886);
        done->Run();
        return;
    }

    const std::string& file_key = keys[keys.size()-1];
    std::string value;
    leveldb::Status s = _name_db->Get(leveldb::ReadOptions(), file_key, &value);
    if (s.ok()) {
        FileInfo* file_info = response->mutable_file_info();
        bool ret = file_info->ParseFromArray(value.data(), value.size());
        int64_t file_size = 0;
        for (int i = 0; i < file_info->blocks_size(); i++) {
            int64_t block_id = file_info->blocks(i);
            BlockManager::NSBlock nsblock(block_id);
            if (!_block_manager->GetBlock(block_id, &nsblock)) {
                continue;
            }
            file_size += nsblock.block_size;
        }
        assert(ret);
        file_info->set_size(file_size);
        response->set_status(0);
        LOG(INFO, "Stat: %s return: %ld\n", path.c_str(), file_size);
    } else {
        LOG(WARNING, "Stat: %s return: not found\n", path.c_str());
        response->set_status(-1);
    }
    done->Run();
}

void NameServerImpl::Rename(::google::protobuf::RpcController* controller,
                            const ::bfs::RenameRequest* request,
                            ::bfs::RenameResponse* response,
                            ::google::protobuf::Closure* done) {
    response->set_sequence_id(request->sequence_id());
    const std::string& oldpath = request->oldpath();
    const std::string& newpath = request->newpath();
    LOG(INFO, "Rename: %s to %s\n", oldpath.c_str(), newpath.c_str());

    /// Should lock something?
    std::vector<std::string> oldkeys, newkeys;
    if (!SplitPath(oldpath, &oldkeys) || !SplitPath(newpath, &newkeys)) {
        LOG(WARNING, "Rename SplitPath fail: %s, %s\n", oldpath.c_str(), newpath.c_str());
        response->set_status(886);
        done->Run();
        return;
    }
    
    const std::string& old_key = oldkeys[oldkeys.size()-1];
    const std::string& new_key = newkeys[newkeys.size()-1];
    std::string value;
    leveldb::Status s = _name_db->Get(leveldb::ReadOptions(), new_key, &value);
    // New file must be not found
    if (s.IsNotFound()) {
        s = _name_db->Get(leveldb::ReadOptions(), old_key, &value);
        if (s.ok()) {
            FileInfo file_info;
            bool ret = file_info.ParseFromArray(value.data(), value.size());
            assert(ret);
            // Directory rename is not impliment.
            if ((file_info.type() & (1<<9)) == 0) {
                leveldb::WriteBatch batch;
                batch.Put(new_key, value);
                batch.Delete(old_key);
                s = _name_db->Write(leveldb::WriteOptions(), &batch);
                if (s.ok()) {
                    response->set_status(0);
                    done->Run();
                    return;
                } else {
                    LOG(WARNING, "Rename write leveldb fail\n");
                }
            } else {
                LOG(WARNING, "Rename not support directory\n");
            }
        } else {
            LOG(WARNING, "Rename not found: %s\n", oldpath.c_str());
        }
    } else {
        LOG(WARNING, "Rename target file %s is existent\n", newpath.c_str());
    }
    response->set_status(886);
    done->Run();
}

void NameServerImpl::Unlink(::google::protobuf::RpcController* controller,
                            const ::bfs::UnlinkRequest* request,
                            ::bfs::UnlinkResponse* response,
                            ::google::protobuf::Closure* done) {
    response->set_sequence_id(request->sequence_id());
    const std::string& path = request->path();
    LOG(INFO, "Unlink: %s\n", path.c_str());

    int ret_status = 886;
    std::vector<std::string> keys;
    if (!SplitPath(path, &keys)) {
        LOG(WARNING, "Unlink SplitPath fail: %s\n", path.c_str());
        response->set_status(ret_status);
        done->Run();
        return;
    }

    const std::string& file_key = keys[keys.size()-1];
    std::string value;
    leveldb::Status s = _name_db->Get(leveldb::ReadOptions(), file_key, &value);
    if (s.ok()) {
        FileInfo file_info;
        bool ret = file_info.ParseFromArray(value.data(), value.size());
        assert(ret);
        // Only support file
        if ((file_info.type() & (1<<9)) == 0) {
            for (int i = 0; i < file_info.blocks_size(); i++) {
                std::set<int32_t> chunkservers;
                _block_manager->GetReplicaLocation(file_info.blocks(i), &chunkservers);
                std::set<int32_t>::iterator it = chunkservers.begin();
                for (; it != chunkservers.end(); ++it) {
                    _block_manager->MarkObsoleteBlock(file_info.blocks(i), *it);
                }
                char idstr[64];
                snprintf(idstr, sizeof(idstr), "%13ld", file_info.blocks(i));
                s = _block_db->Delete(leveldb::WriteOptions(), idstr);
                if (s.ok()) {
                    LOG(INFO, "Remove block done: %s\n", idstr);
                } else {
                    LOG(WARNING, "Remove block fail: %s\n", idstr);
                }
            }
            s = _name_db->Delete(leveldb::WriteOptions(), file_key);
            if (s.ok()) {
                LOG(INFO, "Unlink done: %s\n", path.c_str());
                ret_status = 0;
            } else {
                LOG(WARNING, "Unlink write meta fail: %s\n", path.c_str());
            }
        } else {
            LOG(WARNING, "Unlink not support directory: %s\n", path.c_str());
        }
    } else if (s.IsNotFound()) {
        LOG(WARNING, "Unlink not found: %s\n", path.c_str());
        ret_status = 0;
    }
    
    response->set_status(ret_status);
    done->Run();
}

void NameServerImpl::DeleteDirectory(::google::protobuf::RpcController* controller,
                                     const ::bfs::DeleteDirectoryRequest* request,
                                     ::bfs::DeleteDirectoryResponse* response,
                                     ::google::protobuf::Closure* done)  {
    response->set_sequence_id(request->sequence_id());
    std::string path = request->path();
    bool recursive = request->recursive();
    if (path.empty() || path[0] != '/') {
        response->set_status(886);
        done->Run();
    }

    int ret_status = DeleteDirectoryRecursive(path, recursive);

    response->set_status(ret_status);
    done->Run();
}

int NameServerImpl::DeleteDirectoryRecursive(std::string& path, bool recursive) {
    int ret_status = 0;
    std::vector<std::string> keys;

    if (!SplitPath(path, &keys)) {
        LOG(WARNING, "Delete Directory SplitPath fail: %s\n", path.c_str());
        ret_status = 886;
        return ret_status;
    }
    std::string dentry_key = keys[keys.size() - 1];
    {
        std::string value;
        leveldb::Status s = _name_db->Get(leveldb::ReadOptions(), dentry_key, &value);
        if (!s.ok()) {
            LOG(INFO, "Delete Directory, %s is not found.", dentry_key.data() + 2);
            return 404;
        }
    }
    
    keys.clear();
    if (path[path.size() - 1] != '/') {
        path += '/';
    }
    path += '#';

    if (!SplitPath(path, &keys)) {
        LOG(WARNING, "Delete Directory SplitPath fail: %s\n", path.c_str());
        ret_status = 886;
        return ret_status;
    }
    const std::string& file_start_key = keys[keys.size() - 1];
    std::string file_end_key = file_start_key;
    if (file_end_key[file_end_key.size() - 1] == '#') {
        file_end_key[file_end_key.size() - 1] = '\255';
    } else {
        file_end_key += '#';
    }

    leveldb::Iterator* it = _name_db->NewIterator(leveldb::ReadOptions());
    it->Seek(file_start_key);
    if (it->Valid() && recursive == false) {
        LOG(WARNING, "Try to delete an unempty directory unrecursively: %s\n", dentry_key.c_str());
        delete it;
        ret_status = 886;
        return ret_status;
    }

    for (; it->Valid(); it->Next()) {
        leveldb::Slice key = it->key();
        if (key.compare(file_end_key) >= 0) {
            break;
        }
        FileInfo file_info;
        bool ret = file_info.ParseFromArray(it->value().data(), it->value().size());
        assert(ret);
        if ((file_info.type() & (1 << 9)) != 0) {
            std::string dir_path(std::string(key.data() + 2, key.size() - 2));
            LOG(INFO, "Recursive to path: %s\n", dir_path.c_str());
            ret_status = DeleteDirectoryRecursive(dir_path, recursive);
            if (ret_status != 0) {
                break;
            }
        } else {
            for (int i = 0; i < file_info.blocks_size(); i++) {
                std::set<int32_t> chunkservers;
                _block_manager->GetReplicaLocation(file_info.blocks(i), &chunkservers);
                std::set<int32_t>::iterator it = chunkservers.begin();
                for (; it!= chunkservers.end(); ++it) {
                    _block_manager->MarkObsoleteBlock(file_info.blocks(i), *it);
                }
            }
            leveldb::Status s = _name_db->Delete(leveldb::WriteOptions(), std::string(key.data(), key.size()));
            if (s.ok()) {
                LOG(INFO, "Unlink file done: %s\n", std::string(key.data() + 2, key.size() - 2).c_str());
            } else {
                LOG(WARNING, "Unlink file fail: %s\n", std::string(key.data() + 2, key.size() - 2).c_str());
                ret_status = 886;
                break;
            }
        }
    }

    if (ret_status == 0) {
        leveldb::Status s = _name_db->Delete(leveldb::WriteOptions(), dentry_key);
        if (s.ok()) {
            LOG(INFO, "Unlink dentry done: %s\n", dentry_key.c_str() + 2);
        } else {
            LOG(INFO, "Unlink dentry fail: %s\n", dentry_key.c_str() + 2);
            ret_status = 886;
        }
    }
    delete it;
    return ret_status;
}

void NameServerImpl::ChangeReplicaNum(::google::protobuf::RpcController* controller,
                                      const ::bfs::ChangeReplicaNumRequest* request,
                                      ::bfs::ChangeReplicaNumResponse* response,
                                      ::google::protobuf::Closure* done) {
    response->set_sequence_id(request->sequence_id());
    std::string file_name = request->file_name();
    int32_t replica_num = request->replica_num();
    std::vector<std::string> keys;
    int ret_status = 886;

    if (!SplitPath(file_name, &keys)) {
        LOG(WARNING, "Change replica num SplitPath fail: %s\n", file_name.c_str());
        response->set_status(ret_status);
        done->Run();
        return;
    }

    const std::string& file_key = keys[keys.size() - 1];
    std::string info_value;
    leveldb::Status s = _name_db->Get(leveldb::ReadOptions(), file_key, &info_value);
    if (s.ok()) {
        FileInfo file_info;
        bool ret = file_info.ParseFromArray(info_value.data(), info_value.size());
        assert(ret);
        file_info.set_replicas(replica_num);
        file_info.SerializeToString(&info_value);
        s = _name_db->Put(leveldb::WriteOptions(), file_key, info_value);
        assert(s.ok());
        int64_t block_id = file_info.blocks(0);
        if (_block_manager->ChangeReplicaNum(block_id, replica_num)) {
            LOG(INFO, "Change %s replica num to %d\n", file_name.c_str(), replica_num);
            ret_status = 0;
        } else {
            ret_status = 886;
        }
    } else if (s.IsNotFound()) {
        LOG(WARNING, "Change replica num not found: %s\n", file_name.c_str());
        ret_status = 404;
    }
    response->set_status(ret_status);
    done->Run();
}
}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
