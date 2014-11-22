// Copyright (c) 2014, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>

#include <leveldb/db.h>
#include <leveldb/cache.h>

#include "chunkserver_impl.h"
#include "proto/nameserver.pb.h"
#include "common/mutex.h"
#include "chunkserver_client.h"
#include "common/atomic.h"
#include "common/mutex.h"
#include "common/util.h"
#include "common/timer.h"

extern std::string FLAGS_block_store_path;
extern std::string FLAGS_nameserver;
extern std::string FLAGS_chunkserver_port;
extern int32_t FLAGS_heartbeat_interval;
extern int32_t FLAGS_blockreport_interval;

namespace bfs {

class Block {
public:
    Block(int64_t block_id, const std::string diskfile= "", int64_t block_size= 0) :
      _block_id(block_id), _blockbuf(NULL), _buflen(0),
      _datalen(block_size), _disk_file(diskfile), _file_desc(-1), _refs(0) {
        if (diskfile != "") {
            _type = InDisk;
        } else {
            _type = InMem;
        }
    }
    ~Block() {
        delete _blockbuf;
        _blockbuf = NULL;
        _buflen = 0;
        _datalen = 0;
        if (_file_desc >= 0) {
            close(_file_desc);
            _file_desc = -1;
        }
        printf("Block %ld deleted\n", _block_id);
    }
    int64_t Id() const {
        return _block_id;
    }
    int64_t Size() const {
        return _datalen;
    }
    int32_t Read(char* buf, int32_t len, int64_t offset) {
        /// Raw impliment, no concurrency
        MutexLock lock(&_mu);
        if (_type == InMem) {
            int64_t left = _datalen - offset;
            if (left < 0) {
                return 0;
            }
            if (left > len) {
                left = len;
            }
            memcpy(buf, _blockbuf + offset, left);
            return left;
        }        
        if (_file_desc == -1) {
            int fd  = open(_disk_file.c_str(), O_RDONLY);
            if (fd < 0) {
                fprintf(stderr, "Open block [%s] for read fail: %s\n",
                    _disk_file.c_str(), strerror(errno));
                return -2;
            }
            _file_desc = fd;
        }
        return pread(_file_desc, buf, len, offset);
    }
    bool Writeable() {
        return (_type == InMem);
    }
    int64_t Append(const char*buf, int32_t len) {
        assert (_type == InMem);
        MutexLock lock(&_mu);
        if (_blockbuf == NULL) {
            _buflen = std::max(len*2, 1024);
            _blockbuf = new char[_buflen];
        } else if (_datalen + len > _buflen) {
            _buflen = std::max(_buflen * 2, _datalen + len);
            char* newbuf = new char[_buflen];
            memcpy(newbuf, _blockbuf, _datalen);
            delete _blockbuf;
            _blockbuf = newbuf;
        }
        memcpy(_blockbuf + _datalen, buf, len);
        _datalen += len;
        return _datalen;
    }
    bool FlushToDisk(const std::string& path) {
        assert (_type == InMem);
        MutexLock lock(&_mu);
        bool ret = false;
        FILE* fp = fopen(path.c_str(), "wb");
        if (fp == NULL) {
            fprintf(stderr, "Open %s for flush fail\n", path.c_str());
        } else if (fwrite(_blockbuf, _datalen , 1, fp) != 1) {
            fprintf(stderr, "Write to disk fail: %s\n", path.c_str());
        } else {
            ret = true;
        }
        if (fp) {
            fsync(fileno(fp));
            fclose(fp);
        }
        _disk_file = path;
        _type = InDisk;
        delete _blockbuf;
        _blockbuf = NULL;
        _buflen = 0;
        return ret;
    }
    void AddRef() {
        common::atomic_inc(&_refs);
    }
    void DecRef() {
        if (common::atomic_add(&_refs, -1) == 1) {
            delete this;
        }
    }
private:
    enum Type {
        InDisk,
        InMem,
    };
    int64_t     _block_id;
    char*       _blockbuf;
    int64_t     _buflen;
    int64_t     _datalen;
    std::string _disk_file;
    int         _file_desc; ///< disk file fd
    Type        _type;      ///< disk or mem
    volatile int _refs;
    Mutex       _mu;
};

class BlockManager {
public:
    struct BlockMeta {
        int64_t block_id;
        int64_t block_size;
        int64_t checksum;
        char    file_name[16];  // format: /XXX/XXXXXXXXXX not more than 15bytes, index 10^13block
    };
    BlockManager(std::string store_path) 
        :_store_path(store_path), _metadb(NULL), _block_num(0) {
    }
    ~BlockManager() {
        for (BlockMap::iterator it = _block_map.begin();
                it != _block_map.end(); ++it) {
            it->second->DecRef();
        }
        _block_map.clear();
        delete _metadb;
        _metadb = NULL;
    }
    bool LoadStorage() {
        assert (_block_num == 0);
        MutexLock lock(&_mu);
        leveldb::Options options;
        options.create_if_missing = true;
        leveldb::Status s = leveldb::DB::Open(options, _store_path+"/meta/", &_metadb);
        if (!s.ok()) {
            fprintf(stderr, "Load blocks fail\n");
            return false;
        }
        leveldb::Iterator* it = _metadb->NewIterator(leveldb::ReadOptions());
        for (it->SeekToFirst(); it->Valid(); it->Next()) {
            int64_t block_id = 0;
            if (1 != sscanf(it->key().data(), "%ld", &block_id)) {
                printf("Unknown key: %s\n", it->key().ToString().c_str());
                delete it;
                return false;
            }
            BlockMeta meta;
            assert(it->value().size() == sizeof(meta));
            memcpy(&meta, it->value().data(), sizeof(meta));
            assert(meta.block_id == block_id);
            
            Block* block = new Block(block_id, _store_path + meta.file_name, meta.block_size);
            block->AddRef();
            _block_map[block_id] = block;
            _block_num ++;
        }
        delete it;
        printf("Load %ld blocks\n", _block_num);
        return true;
    }
    bool ListBlocks(std::vector<BlockMeta>* blocks) {
        assert(_metadb);
        leveldb::Iterator* it = _metadb->NewIterator(leveldb::ReadOptions());
        for (it->SeekToFirst(); it->Valid(); it->Next()) {
            int64_t block_id = 0;
            if (1 != sscanf(it->key().data(), "%ld", &block_id)) {
                printf("Unknown key: %s\n", it->key().ToString().c_str());
                delete it;
                return false;
            }
            BlockMeta meta;
            assert(it->value().size() == sizeof(meta));
            memcpy(&meta, it->value().data(), sizeof(meta));
            assert(meta.block_id == block_id);
            
            blocks->push_back(meta);            
        }
        return true;
    }
    Block* FindBlock(int64_t block_id, bool create_if_missing) {
        MutexLock lock(&_mu);
        
        Block* block = NULL;
        BlockMap::iterator it = _block_map.find(block_id);
        if (it != _block_map.end()) {
            block = it->second;
        } else if (create_if_missing) {
            ///TODO: LRU block map
            block = new Block(block_id);
            // for block_map
            block->AddRef();
            _block_map[block_id] = block;
        } else {
            // not found
        }
        // for user
        if (block) {
            block->AddRef();
        }
        return block;
    }
    
    bool FinishBlock(Block* block) {
        MutexLock lock(&_mu);
        int64_t block_id = block->Id();
        assert( block_id < (10L<<13));
        
        BlockMeta meta;
        meta.block_size = block->Size();
        meta.checksum = 0;
        meta.block_id = block_id;
        int len = snprintf(meta.file_name, sizeof(meta.file_name),
            "/%03ld", block_id % 1000);
        mkdir((_store_path + meta.file_name).c_str(), 0755);
        len += snprintf(meta.file_name + len, sizeof(meta.file_name) - len,
            "/%010ld", block_id/1000);
        assert (len == 15 && meta.file_name[len] == 0);

        // disk flush & sync
        block->FlushToDisk(_store_path + meta.file_name);

        /// write meta & sync
        char idstr[64];
        snprintf(idstr, sizeof(idstr), "%13ld", block_id);
        
        leveldb::WriteOptions options;
        options.sync = true;
        leveldb::Status s = _metadb->Put(options, idstr,
            leveldb::Slice(reinterpret_cast<char*>(&meta),sizeof(meta)));
        if (!s.ok()) {
            fprintf(stderr, "Write to meta fail:%s\n", idstr);
            return false;
        }
        return true;
    }
private:
    std::string _store_path;
    typedef std::map<int64_t, Block*> BlockMap;
    BlockMap  _block_map;
    leveldb::DB* _metadb;
    int64_t _block_num;
    Mutex   _mu;
};

ChunkServerImpl::ChunkServerImpl()
    : _quit(false), _chunkserver_id(0), _namespace_version(0) {
    _data_server_addr = common::util::GetLocalHostName() + ":" + FLAGS_chunkserver_port;
    _block_manager = new BlockManager(FLAGS_block_store_path);
    bool s_ret = _block_manager->LoadStorage();
    assert(s_ret == true);
    _rpc_client = new RpcClient();
    if (!_rpc_client->GetStub(FLAGS_nameserver, &_nameserver)) {
        assert(0);
    }
    int ret = pthread_create(&_routine_thread, NULL, RoutineWrapper, this);
    assert(ret == 0);
}

ChunkServerImpl::~ChunkServerImpl() {
    _quit = true;
    pthread_join(_routine_thread, NULL);
    delete _block_manager;
    delete _rpc_client;
}

void* ChunkServerImpl::RoutineWrapper(void* arg) {
    reinterpret_cast<ChunkServerImpl*>(arg)->Routine();
    return NULL;
}

void ChunkServerImpl::Routine() {
    static int64_t ticks = 0;
    int64_t next_report = -1;
    while (!_quit) {
        // heartbeat
        if (ticks%FLAGS_heartbeat_interval == 0) {
            HeartBeatRequest request;
            request.set_chunkserver_id(_chunkserver_id);
            request.set_data_server_addr(_data_server_addr);
            request.set_namespace_version(_namespace_version);
            HeartBeatResponse response;
            if (!_rpc_client->SendRequest(_nameserver, &NameServer_Stub::HeartBeat,
                    &request, &response, 5, 1)) {
                printf("Heat beat fail\n");
            } else if (_namespace_version != response.namespace_version()) {
                printf("Connect to nameserver, new chunkserver_id: %d\n",
                    response.chunkserver_id());
                _namespace_version = response.namespace_version();
                _chunkserver_id = response.chunkserver_id();
                next_report = ticks;
            }
        }
        // block report
        if (ticks == next_report) {
            BlockReportRequest request;
            request.set_chunkserver_id(_chunkserver_id);
            request.set_namespace_version(_namespace_version);

            std::vector<BlockManager::BlockMeta> blocks;
            _block_manager->ListBlocks(&blocks);
            for (size_t i = 0; i < blocks.size(); i++) {
                ReportBlockInfo* info = request.add_blocks();
                info->set_block_id(blocks[i].block_id);
                info->set_block_size(blocks[i].block_size);
                info->set_version(0);
            }
            BlockReportResponse response;
            if (!_rpc_client->SendRequest(_nameserver, &NameServer_Stub::BlockReport,
                    &request, &response, 20, 3)) {
                printf("Block reprot fail\n");
                next_report += 60;  // retry
            } else {
                next_report += FLAGS_blockreport_interval;
            }
        }
        ++ ticks;
        sleep(1);
    }
}

bool ChunkServerImpl::ReportFinish(Block* block) {
    BlockReportRequest request;
    request.set_chunkserver_id(_chunkserver_id);
    request.set_namespace_version(_namespace_version);
    
    ReportBlockInfo* info = request.add_blocks();
    info->set_block_id(block->Id());
    info->set_block_size(block->Size());
    info->set_version(0);
    BlockReportResponse response;
    if (!_rpc_client->SendRequest(_nameserver, &NameServer_Stub::BlockReport,
            &request, &response, 20, 3)) {
        printf("Reprot finish fail: %ld\n", block->Id());
        return false;
    }
    printf("Reprot finish %ld\n", block->Id());
    return true;
}

void ChunkServerImpl::WriteBlock(::google::protobuf::RpcController* controller,
                        const WriteBlockRequest* request,
                        WriteBlockResponse* response,
                        ::google::protobuf::Closure* done) {
    response->set_sequence_id(request->sequence_id());
    int64_t block_id = request->block_id();
    const std::string& databuf = request->databuf();
    int64_t offset = request->offset();
    /// search
    printf("WriteBlock [%ld:%ld:%lu] %lu\n", block_id, offset, databuf.size(),
           request->sequence_id());
    Block* block = _block_manager->FindBlock(block_id, true);
    
    if (offset != block->Size() || !block->Writeable()) {
        fprintf(stderr, "Write offset[%ld] block_size[%ld] mismatch or unwriteable\n",
            offset, block->Size());
        block->DecRef();
        response->set_status(0);
        done->Run();
        return;
    } else if(!databuf.empty()) {
        block->Append(databuf.data(), databuf.size());
    }


    if (request->chunkservers_size()) {
        ChunkServerClient next_chunkserver(_rpc_client, request->chunkservers(0));
        WriteBlockRequest next_request(*request);
        next_request.clear_chunkservers();
        for (int i = 1; i < request->chunkservers_size(); i++) {
            next_request.add_chunkservers(request->chunkservers(i));
        }
        int64_t seq = next_request.sequence_id();
        printf("Writeblock send [%ld:%ld:%lu] %ld to next %s\n", 
            block_id, offset, databuf.size(), seq, request->chunkservers(0).c_str());
        bool ret = next_chunkserver.SendRequest(&ChunkServer_Stub::WriteBlock, 
            &next_request, response, 5, 3);
        if (!ret && !response->has_bad_chunkserver()) {
            response->set_bad_chunkserver("self address");
        }
    }
    if (!response->has_status()) {
        response->set_status(0);
    }
    printf("WriteBlock done [%ld:%ld:%lu]\n", block_id, offset, databuf.size());
    if (request->is_last()) {
        printf("WriteBlock block finish [%ld:%ld]\n", block_id, block->Size());
        _block_manager->FinishBlock(block);
        ReportFinish(block);
    }
    block->DecRef();
    block = NULL;
    done->Run();
}
void ChunkServerImpl::ReadBlock(::google::protobuf::RpcController* controller,
                        const ReadBlockRequest* request,
                        ReadBlockResponse* response,
                        ::google::protobuf::Closure* done) {
    int64_t block_id = request->block_id();
    int64_t offset = request->offset();
    int32_t read_len = request->read_len();

    int status = 0;

    printf("ReadBlock: %ld offset: %ld len: %d\n", block_id, offset, read_len);
    Block* block = _block_manager->FindBlock(block_id, false);
    if (block == NULL) {
        status = 404;
    } else {
        char* buf = new char[read_len];
        int32_t len = block->Read(buf, read_len, offset);
        if (len >= 0) {
            response->mutable_databuf()->assign(buf, len);
        } else {
            status = 882;
        }
        delete buf;
    }
    response->set_status(status);
    done->Run();
}

} // namespace bfs


/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
