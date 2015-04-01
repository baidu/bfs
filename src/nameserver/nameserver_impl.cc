// Copyright (c) 2014, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#include <set>

#include <sofa/pbrpc/pbrpc.h>
#include <leveldb/db.h>
#include <leveldb/cache.h>
#include <leveldb/write_batch.h>

#include "nameserver_impl.h"
#include "common/mutex.h"
#include "common/timer.h"
#include "common/logging.h"

extern std::string FLAGS_namedb_path;
extern int64_t FLAGS_namedb_cache_size;

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

class ChunkServerManager {
public:
    ChunkServerManager() 
        : next_chunkserver_id(1) {
    }
    void HanldeHeartBeat(const HeartBeatRequest* request, HeartBeatResponse* response) {
        MutexLock lock(&_mu);
        int32_t id = request->chunkserver_id();
        ChunkServerInfo* info = _chunkservers[id];
        assert(info);
        int32_t now_time = time(NULL);
        _heartbeat_list.erase(info->last_heartbeat());
        _heartbeat_list[now_time] = info;
        info->set_last_heartbeat(now_time);
    }
    bool GetChunkServerChains(int num, 
                              std::vector<std::pair<int32_t,std::string> >* chains) {
        MutexLock lock(&_mu);
        if (num > static_cast<int>(_heartbeat_list.size())) {
            LOG(WARNING, "not enough alive chunkservers for GetChunkServerChains\n");
            return false;
        }
        std::map<int32_t, ChunkServerInfo*>::const_reverse_iterator it = _heartbeat_list.rbegin();
        std::vector<std::pair<int32_t, int64_t> > chunkserver_load;
        std::vector<std::pair<int32_t, int64_t> >::iterator load_it;

        //insert sort according chunkserver load
        while(it != _heartbeat_list.rend()) {
            for (load_it = chunkserver_load.begin(); load_it != chunkserver_load.end(); load_it++) {
                if (it->second->data_size() < load_it->second)
                    break;
            }
            chunkserver_load.insert(load_it, std::make_pair(it->first, it->second->data_size()));

            it++;
        }

        load_it = chunkserver_load.begin();
        for (int i = 0; i < num; ++i, ++load_it) {
            ChunkServerInfo* cs = _heartbeat_list[load_it->first];
            chains->push_back(std::make_pair(cs->id(), cs->address()));
        }

        return true;
    }
    int64_t AddChunkServer(const std::string& address) {
        MutexLock lock(&_mu);
        int64_t id = next_chunkserver_id++;
        ChunkServerInfo* info = new ChunkServerInfo;
        info->set_id(id);
        info->set_address(address);
        _chunkservers[id] = info;
        int32_t now_time = time(NULL);
        _heartbeat_list[now_time] = info;
        info->set_last_heartbeat(now_time);
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
            LOG(WARNING, "ChunkServer does not exist!, chunkserver id: %ld\n", id); 
            assert(0);
            return false;
        } else {
            it->second->set_data_size(size);
            LOG(INFO, "Get Report of ChunkServerLoad, server id: %ld, load: %ld\n", id, size);
            return true;
        }
    }

private: 
    Mutex _mu;      /// _chunkservers list mutext;
    typedef std::map<int32_t, ChunkServerInfo*> ServerMap;
    ServerMap _chunkservers;
    std::map<int32_t, ChunkServerInfo*> _heartbeat_list;
    int32_t next_chunkserver_id;
};

class BlockManager {
public:
    struct NSBlock {
        int64_t id;
        int64_t version;
        std::set<int32_t> replica;
        int64_t block_size;
        NSBlock(int64_t block_id) : id(block_id),version(0) {}
        void AddChunkServer(int32_t chunkserver) {};
    };
    BlockManager():_next_block_id(1) {}
    int64_t NewBlock() {
        MutexLock lock(&_mu);
        return ++_next_block_id;
    }
    bool AddBlock(int64_t id, int32_t server_id, int64_t block_size) {
        MutexLock lock(&_mu);
        NSBlock* nsblock = NULL;
        NSBlockMap::iterator it = _block_map.find(id);
        if (it == _block_map.end()) {
            nsblock = new NSBlock(id);
            _block_map[id] = nsblock;
            nsblock->block_size = block_size;
        } else {
            nsblock = it->second;
            if (nsblock->block_size && nsblock->block_size !=  block_size) {
                LOG(WARNING, "block size mismatch, block: %ld\n", id);
                assert(0);
                return false;
            }
        }
        if (_next_block_id <= id) {
            _next_block_id = id + 1;
        }
        /// 增加一个副本, 无论之前已经有几个了, 多余的通过gc处理
        nsblock->replica.insert(server_id);
        return true;
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
private:
    Mutex _mu;
    typedef std::map<int64_t, NSBlock*> NSBlockMap;
    NSBlockMap _block_map;
    int64_t _next_block_id;
};

NameServerImpl::NameServerImpl() {
    leveldb::Options options;
    options.create_if_missing = true;
    options.block_cache = leveldb::NewLRUCache(FLAGS_namedb_cache_size*1024L*1024L);
    leveldb::Status s = leveldb::DB::Open(options, FLAGS_namedb_path, &_db);
    if (!s.ok()) {
        _db = NULL;
        LOG(FATAL, "Open leveldb fail: %s\n", s.ToString().c_str());
    }
    _namespace_version = common::timer::get_micros();
    _chunkserver_manager = new ChunkServerManager();
    _block_manager = new BlockManager();
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
        _chunkserver_manager->HanldeHeartBeat(request, response);
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
    LOG(INFO, "Report from %d, %d blocks\n", id, request->blocks_size());
    if (version != _namespace_version) {
        response->set_status(8882);
    } else {
        const ::google::protobuf::RepeatedPtrField<ReportBlockInfo>& blocks = request->blocks();
        int64_t size = 0;
        for (int i = 0; i < blocks.size(); i++) {
            const ReportBlockInfo& block =  blocks.Get(i);
            _block_manager->AddBlock(block.block_id(), id, block.block_size());
            size += block.block_size();
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
    std::vector<std::string> file_keys;
    if (!SplitPath(file_name, &file_keys)) {
        response->set_status(886);
        done->Run();
        return;
    }
    FileInfo file_info;
    std::string info_value;
    int depth = file_keys.size();
    leveldb::Status s;
    for (int i=0; i < depth-1; ++i) {
        s = _db->Get(leveldb::ReadOptions(), file_keys[i], &info_value);
        if (s.IsNotFound()) {
            file_info.set_type((1<<9)|0755);
            file_info.set_ctime(time(NULL));
            file_info.SerializeToString(&info_value);
            s = _db->Put(leveldb::WriteOptions(), file_keys[i], info_value);
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
    s = _db->Get(leveldb::ReadOptions(), file_key, &info_value);
    if (s.IsNotFound()) {
        if (request->type() == 0) {
            file_info.set_type(0755);
        } else {
            file_info.set_type(request->type());
        }
        file_info.set_id(0);
        file_info.set_ctime(time(NULL));
        //file_info.add_blocks();
        file_info.SerializeToString(&info_value);
        s = _db->Put(leveldb::WriteOptions(), file_key, info_value);
        if (s.ok()) {
            LOG(INFO, "CreateFile %s\n", file_key.c_str());
            response->set_status(0);
        } else {
            LOG(WARNING, "CreateFile %s\n fail: Put fail", file_key.c_str());
            response->set_status(2);
        }
    } else {
        LOG(WARNING, "CreateFile %s fail: already exist!\n", file_name.c_str());
        response->set_status(1);
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
    leveldb::Status s = _db->Get(leveldb::ReadOptions(), file_key, &infobuf);
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
    int replica_num = 2;
    /// check lease for write
    std::vector<std::pair<int32_t, std::string> > chains;
    if (_chunkserver_manager->GetChunkServerChains(replica_num, &chains)) {
        int64_t new_block_id = _block_manager->NewBlock();
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
        s = _db->Put(leveldb::WriteOptions(), file_key, infobuf);
        assert(s.ok());
    } else {
        response->set_status(886);
    }
    done->Run();
}

void NameServerImpl::FinishBlock(::google::protobuf::RpcController* controller,
                         const FinishBlockRequest*,
                         FinishBlockResponse*,
                         ::google::protobuf::Closure* done) {
    controller->SetFailed("Method FinishBlock() not implemented.");
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
    leveldb::Status s = _db->Get(leveldb::ReadOptions(), file_key, &infobuf);
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
                    LOG(INFO, "return server %s\n", addr.c_str());
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
    path += "#";

    common::timer::AutoTimer at(0, "ListDirectory", path.c_str());
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

    common::timer::AutoTimer at1(0, "ListDirectory iterate", path.c_str());
    //printf("List Directory: %s, return: ", file_start_key.c_str());
    leveldb::Iterator* it = _db->NewIterator(leveldb::ReadOptions());
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
    
    common::timer::AutoTimer at2(0, "ListDirectory done run", path.c_str());
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
    leveldb::Status s = _db->Get(leveldb::ReadOptions(), file_key, &value);
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
    leveldb::Status s = _db->Get(leveldb::ReadOptions(), new_key, &value);
    // New file must be not found
    if (s.IsNotFound()) {
        s = _db->Get(leveldb::ReadOptions(), old_key, &value);
        if (s.ok()) {
            FileInfo file_info;
            bool ret = file_info.ParseFromArray(value.data(), value.size());
            assert(ret);
            // Directory rename is not impliment.
            if ((file_info.type() & (1<<9)) == 0) {
                leveldb::WriteBatch batch;
                batch.Put(new_key, value);
                batch.Delete(old_key);
                s = _db->Write(leveldb::WriteOptions(), &batch);
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
        LOG(WARNING, "Rename before delete %s\n", newpath.c_str());
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
    leveldb::Status s = _db->Get(leveldb::ReadOptions(), file_key, &value);
    if (s.ok()) {
        FileInfo file_info;
        bool ret = file_info.ParseFromArray(value.data(), value.size());
        assert(ret);
        // Only support file
        if ((file_info.type() & (1<<9)) == 0) {
            s = _db->Delete(leveldb::WriteOptions(), file_key);
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
}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
