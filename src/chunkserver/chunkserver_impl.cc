// Copyright (c) 2014, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#include <errno.h>
#include <fcntl.h>
#include <string.h>
#include <sys/stat.h>

#include <boost/bind.hpp>
#include <gflags/gflags.h>

#include <leveldb/db.h>
#include <leveldb/cache.h>

#include "chunkserver_impl.h"
#include "rpc/rpc_client.h"
#include "proto/nameserver.pb.h"
#include "common/mutex.h"
#include "common/atomic.h"
#include "common/counter.h"
#include "common/mutex.h"
#include "common/util.h"
#include "common/timer.h"
#include "common/sliding_window.h"
#include "common/logging.h"

DECLARE_string(block_store_path);
DECLARE_string(nameserver);
DECLARE_string(nameserver_port);
DECLARE_string(chunkserver_port);
DECLARE_int32(heartbeat_interval);
DECLARE_int32(blockreport_interval);

namespace bfs {

common::Counter g_block_buffers;
common::Counter g_blocks;
common::Counter g_writing_bytes;

struct Buffer {
    const char* data_;
    int32_t len_;
    Buffer(const char* buff, int32_t len)
      : data_(buff), len_(len) {}
    Buffer()
      : data_(NULL), len_(0) {}
    Buffer(const Buffer& o)
      : data_(o.data_), len_(o.len_) {}
};

/// Meta of a data block
struct BlockMeta {
    int64_t block_id;
    int64_t block_size;
    int64_t checksum;
    char    file_path[16];  // format: /XXX/XXXXXXXXXX not more than 15bytes, index 10^13block
    BlockMeta()
      : block_id(0), block_size(0), checksum(0) {
        file_path[0] = 0;
    }
};

/// Data block
class Block {
public:
    Block(const BlockMeta& meta, const std::string& store_path) :
      _meta(meta),
      _last_seq(-1), _slice_num(-1), _blockbuf(NULL), _buflen(0),
      _bufdatalen(0), _file_desc(-1), _refs(0),
      _recv_window(NULL), _finished(false) {
        _disk_file = store_path + _meta.file_path;
        g_blocks.Inc();
    }
    ~Block() {
        if (_bufdatalen > 0) {
            LOG(WARNING, "Data lost, %d bytes in %s,%ld",
                _bufdatalen, _meta.file_path, _meta.block_size - _bufdatalen);
        }
        if (_blockbuf) {
            g_block_buffers.Dec();
            delete[] _blockbuf;
            _blockbuf = NULL;
        }
        _buflen = 0;
        _bufdatalen = 0;
        if (_file_desc >= 0) {
            close(_file_desc);
            _file_desc = -1;
        }
        if (_recv_window) {
            if (_recv_window->Size()) {
                fprintf(stderr, "recv_window fragments: %d\n",  _recv_window->Size());
                std::vector<std::pair<int32_t,Buffer> > frags;
                _recv_window->GetFragments(&frags);
                for (uint32_t i = 0; i < frags.size(); i++) {
                    delete[] frags[i].second.data_;
                }
            }
            delete _recv_window;
        }
        LOG(INFO, "Block %ld deleted\n", _meta.block_id);
        g_blocks.Dec();
    }
    /// Getter
    int64_t Id() const {
        return _meta.block_id;
    }
    int64_t Size() const {
        return _meta.block_size;
    }
    std::string GetFilePath() const {
        return _disk_file;
    }
    BlockMeta GetMeta() const {
        return _meta;
    }

    /// Init this block before write.
    bool InitForWrite() {
        int fd  = open(_disk_file.c_str(), O_RDWR | O_CREAT | O_TRUNC, S_IRUSR);
        if (fd < 0) {
            LOG(WARNING, "Open block file %s fail", _disk_file.c_str());
            return false;
        }
        _file_desc = fd;
        _recv_window = new common::SlidingWindow<Buffer>(100,
                       boost::bind(&Block::WriteCallback, this, _1, _2));
        return true;
    }
    /// Set expected slice num, for IsComplete.
    void SetSliceNum(int32_t num) {
        _slice_num = num;
    }
    /// Is all slice is arrival(Notify by the sliding window) 
    bool IsComplete() {
        return (_slice_num == _last_seq + 1);
    }
    /// Mark this block is finish, return iff the first.
    bool MarkFinish() {
        MutexLock lock(&_mu);
        if (_finished) {
            return false;
        }
        _finished = true;
        return true;
    }
    /// Read operation.
    int32_t Read(char* buf, int32_t len, int64_t offset) {
        MutexLock lock(&_mu);
        if (offset > _meta.block_size) {
            return -1;
        }
        int readlen = 0;
        if (_file_desc == -1) {
            int fd  = open(_disk_file.c_str(), O_RDONLY);
            if (fd < 0) {
                fprintf(stderr, "Open block [%s] for read fail: %s\n",
                    _disk_file.c_str(), strerror(errno));
                return -2;
            }
            _file_desc = fd;
        }
        ///
        int64_t diskfile_size = _meta.block_size - _bufdatalen;
        if (offset < diskfile_size) {
            int pread_len = std::min(len, static_cast<int>(diskfile_size - offset));
            readlen = pread(_file_desc, buf, pread_len, offset);
            if(readlen < pread_len) {
                assert(_meta.block_size == offset + readlen);
            }
        }
        int mem_offset = offset - diskfile_size + readlen;
        if (mem_offset >= 0 && mem_offset < _bufdatalen) {
            int mlen = std::min(_bufdatalen - mem_offset, len - readlen);
            memcpy(buf + readlen, _blockbuf + mem_offset, mlen);
            readlen += mlen;
        }
        return readlen;
    }
    /// Write operation.
    bool Write(int32_t seq, int64_t offset, const char* data, int32_t len) {
        LOG(INFO, "Block Write %d\n", seq);
        if (offset < _meta.block_size) {
            LOG(WARNING, "Write a finish block %ld, seq: %d, offset: %ld",
                _meta.block_id, seq, offset);
            return true;
        }
        char* buf = NULL;
        if (len) {
            buf = new char[len];
            memcpy(buf, data, len);
            g_writing_bytes.Add(len);
        }
        int ret = _recv_window->Add(seq, Buffer(buf, len));
        if (ret != 0) {
            delete[] buf;
            g_writing_bytes.Sub(len);
        }
        return (ret >= 0);
    }
    /// Flush block to disk.
    bool Flush() {
        LOG(INFO, "Block %ld Flush to %s", _meta.block_id, _disk_file.c_str());
        MutexLock lock(&_mu);
        if (_bufdatalen) {
            int ret = DiskWrite(_blockbuf, _bufdatalen);
            assert(ret);
            _bufdatalen = 0;
        }
        return true;
    }
    void Close() {
        MutexLock lock(&_mu);
        ///TODO: Error handling
        int ret = close(_file_desc);
        assert(ret == 0);
        _file_desc = -1;
        delete _blockbuf;
        _blockbuf = NULL;
        g_block_buffers.Dec();
        LOG(INFO, "Block %ld %s closed", _meta.block_id, _meta.file_path);
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
    /// Invoke by slidingwindow, when next buffer arrive.
    void WriteCallback(int32_t seq, Buffer buffer) {
        Append(seq, buffer.data_, buffer.len_);
        //LOG(INFO, "Append done [seq:%d, %ld:%d]\n", seq, _blocksize, buffer.len_);
        delete[] buffer.data_;
        g_writing_bytes.Sub(buffer.len_);
    }
    /// Write data to disk
    int DiskWrite(const char* buf, int32_t len) {
        _mu.AssertHeld();
        int wlen = 0;
        while (wlen < len) {
            int w = write(_file_desc, buf + wlen, len - wlen);
            if (w < 0) {
                LOG(WARNING, "IOEroro write %s return %s",
                    _meta.file_path, strerror(errno));
                assert(0);
                return wlen;
            }
            wlen += w;
        }
        return len;
    }
    /// Append to block buffer
    void Append(int32_t seq, const char*buf, int32_t len) {
        MutexLock lock(&_mu);
        if (_blockbuf == NULL) {
            _buflen = 1*1024*1024;
            _blockbuf = new char[_buflen];
            g_block_buffers.Inc();
        }
        int ap_len = len;
        while (_bufdatalen + ap_len > _buflen) {
            int wlen = _buflen - _bufdatalen;
            memcpy(_blockbuf + _bufdatalen, buf, wlen);
            int ret = DiskWrite(_blockbuf, _buflen);
            assert(ret == _buflen);
            _bufdatalen = 0;
            buf += wlen;
            ap_len -= wlen;
        }
        if (ap_len) {
            memcpy(_blockbuf + _bufdatalen, buf, ap_len);
            _bufdatalen += ap_len;
        }
        _meta.block_size += len;
        _last_seq = seq;
    }
private:
    enum Type {
        InDisk,
        InMem,
    };
    BlockMeta   _meta;
    int32_t     _last_seq;
    int32_t     _slice_num;
    char*       _blockbuf;
    int64_t     _buflen;
    int32_t     _bufdatalen;
    std::string _disk_file;
    int         _file_desc; ///< disk file fd
    volatile int _refs;
    Mutex       _mu;
    common::SlidingWindow<Buffer>* _recv_window;
    bool        _finished;
    int         _write_mode;
};

class BlockManager {
public:
    BlockManager(std::string store_path)
        :_store_path(store_path), _metadb(NULL), _block_num(0) {
        ///TODO: Multi disk support
        if (_store_path.empty() || _store_path[_store_path.size() - 1] != '/') {
            _store_path += "/";
        }
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
            LOG(WARNING, "Load blocks fail");
            return false;
        }
        leveldb::Iterator* it = _metadb->NewIterator(leveldb::ReadOptions());
        for (it->SeekToFirst(); it->Valid(); it->Next()) {
            int64_t block_id = 0;
            if (1 != sscanf(it->key().data(), "%ld", &block_id)) {
                LOG(WARNING, "Unknown key: %s\n", it->key().ToString().c_str());
                delete it;
                return false;
            }
            BlockMeta meta;
            assert(it->value().size() == sizeof(meta));
            memcpy(&meta, it->value().data(), sizeof(meta));
            assert(meta.block_id == block_id);

            Block* block = new Block(meta, _store_path);
            block->AddRef();
            _block_map[block_id] = block;
            _block_num ++;
        }
        delete it;
        LOG(INFO, "Load %ld blocks\n", _block_num);
        return true;
    }
    bool ListBlocks(std::vector<BlockMeta>* blocks) {
        assert(_metadb);
        leveldb::Iterator* it = _metadb->NewIterator(leveldb::ReadOptions());
        for (it->SeekToFirst(); it->Valid(); it->Next()) {
            int64_t block_id = 0;
            if (1 != sscanf(it->key().data(), "%ld", &block_id)) {
                LOG(WARNING, "Unknown key: %s\n", it->key().ToString().c_str());
                delete it;
                return false;
            }
            BlockMeta meta;
            assert(it->value().size() == sizeof(meta));
            memcpy(&meta, it->value().data(), sizeof(meta));
            assert(meta.block_id == block_id);

            blocks->push_back(meta);
        }
        delete it;
        return true;
    }
    /// Create a new block for write
    Block* NewBlockForWrite(int64_t block_id) {
        BlockMeta meta;
        meta.block_size = 0;
        meta.checksum = 0;
        meta.block_id = block_id;
        
        assert( block_id < (1L<<40));
        int len = snprintf(meta.file_path, sizeof(meta.file_path),
            "/%03ld", block_id % 1000);
        // Mkdir dir for data block, ignore error, may already exist.
        mkdir((_store_path + meta.file_path).c_str(), 0755);
        len += snprintf(meta.file_path + len, sizeof(meta.file_path) - len,
            "/%010ld", block_id / 1000);
        assert (len == 15 && meta.file_path[len] == 0);

        // Write meta & sync
        if (!SyncBlockMeta(meta)) {
            return NULL;
        }
        
        Block* block = new Block(meta, _store_path);
        if (!block->InitForWrite()) {
            LOG(WARNING, "Block %ld, %s init fail", block_id, meta.file_path);
            delete block;
            return NULL;
        }
        return block;
    }
    Block* FindBlock(int64_t block_id, bool create_if_missing) {
        MutexLock lock(&_mu);

        Block* block = NULL;
        BlockMap::iterator it = _block_map.find(block_id);
        if (it != _block_map.end()) {
            block = it->second;
        } else if (create_if_missing) {
            ///TODO: LRU block map
            block = NewBlockForWrite(block_id);
            ///TODO: Error handling
            assert(block);
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
    bool SyncBlockMeta(const BlockMeta& meta) {
        char idstr[64];
        snprintf(idstr, sizeof(idstr), "%13ld", meta.block_id);

        leveldb::WriteOptions options;
        options.sync = true;
        leveldb::Status s = _metadb->Put(options, idstr,
            leveldb::Slice(reinterpret_cast<const char*>(&meta),sizeof(meta)));
        if (!s.ok()) {
            Log(WARNING, "Write to meta fail:%s\n", idstr);
            return false;
        }
        return true;
    }
    bool CloseBlock(Block* block) {
        MutexLock lock(&_mu);

        if (!block->MarkFinish()) {
            return false;
        }
        // Disk flush & sync
        block->Flush();
        block->Close();
        
        // Update meta
        BlockMeta meta = block->GetMeta();
        return SyncBlockMeta(meta);
    }
    bool RemoveBlock(int64_t block_id) {
        MutexLock lock(&_mu);
        BlockMap::iterator it = _block_map.find(block_id);
        if (it == _block_map.end()) {
            LOG(WARNING, "Try to remove block that does not exist: %ld\n", block_id);
            return false;
        } else {
            Block* block = it->second;
            std::string file_path = block->GetFilePath();

            int ret = remove(file_path.c_str());
            if (ret != 0) {
                LOG(WARNING, "Remove disk file %s fails: %s\n",
                    file_path.c_str(), strerror(errno));
                return false;
            } else {
                LOG(INFO, "Remove disk file done: %s\n", file_path.c_str());
            }

            char dir_name[5];
            snprintf(dir_name, sizeof(dir_name), "/%03ld", block_id % 1000);
            // Rmdir, ignore error when not empty.
            rmdir((_store_path + dir_name).c_str());
            char idstr[14];
            snprintf(idstr, sizeof(idstr), "%13ld", block_id);

            leveldb::Status s = _metadb->Delete(leveldb::WriteOptions(), idstr);
            if (s.ok()) {
                LOG(INFO, "Remove meta info done: %s\n", idstr);
                _block_map.erase(it);
                block->DecRef();
                return true;
            } else {
                LOG(WARNING, "Remove meta info fails: %ld\n", idstr);
                return false;
            }
        }
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
    std::string ns_address = FLAGS_nameserver + ":" + FLAGS_nameserver_port;
    if (!_rpc_client->GetStub(ns_address, &_nameserver)) {
        assert(0);
    }
    _thread_pool = new ThreadPool(10);
    _thread_pool->AddTask(boost::bind(&ChunkServerImpl::LogStatus, this));
    int ret = pthread_create(&_routine_thread, NULL, RoutineWrapper, this);
    assert(ret == 0);
}

ChunkServerImpl::~ChunkServerImpl() {
    _quit = true;
    pthread_join(_routine_thread, NULL);
    _thread_pool->Stop(true);
    delete _thread_pool;
    delete _block_manager;
    delete _rpc_client;
}

void ChunkServerImpl::LogStatus() {
    LOG(INFO, "[Status] blocks %ld, block_buffers %ld, writing_bytes %ld",
        g_blocks.Get(), g_block_buffers.Get(), g_writing_bytes.Get());
    _thread_pool->DelayTask(5000, boost::bind(&ChunkServerImpl::LogStatus, this));
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
        if (ticks % FLAGS_heartbeat_interval == 0) {
            HeartBeatRequest request;
            request.set_chunkserver_id(_chunkserver_id);
            request.set_data_server_addr(_data_server_addr);
            request.set_namespace_version(_namespace_version);
            HeartBeatResponse response;
            if (!_rpc_client->SendRequest(_nameserver, &NameServer_Stub::HeartBeat,
                    &request, &response, 5, 1)) {
                LOG(WARNING, "Heat beat fail\n");
            } else if (_namespace_version != response.namespace_version()) {
                LOG(INFO, "Connect to nameserver, new chunkserver_id: %d\n",
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
            request.set_chunkserver_addr(_data_server_addr);
            request.set_namespace_version(_namespace_version);

            std::vector<BlockMeta> blocks;
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
                LOG(WARNING, "Block reprot fail\n");
                next_report += 60;  // retry
            } else {
                next_report += FLAGS_blockreport_interval;
                //deal with obsolete blocks
                std::vector<int64_t> obsolete_blocks;
                for (int i = 0; i < response.obsolete_blocks_size(); i++) {
                    obsolete_blocks.push_back(response.obsolete_blocks(i));
                }
                boost::function<void ()> task =
                    boost::bind(&ChunkServerImpl::RemoveObsoleteBlocks, this, obsolete_blocks);
                _thread_pool->AddTask(task);

                std::vector<ReplicaInfo> new_replica_info;
                for (int i = 0; i < response.new_replicas_size(); i++) {
                    new_replica_info.push_back(response.new_replicas(i));
                }
                boost::function<void ()> new_replica_task =
                    boost::bind(&ChunkServerImpl::AddNewReplica, this, new_replica_info);
                _thread_pool->AddTask(new_replica_task);

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
        LOG(WARNING, "Reprot finish fail: %ld\n", block->Id());
        return false;
    }

    LOG(INFO, "Reprot finish to nameserver done, block_id: %ld\n", block->Id());
    return true;
}

void ChunkServerImpl::WriteBlock(::google::protobuf::RpcController* controller,
                        const WriteBlockRequest* request,
                        WriteBlockResponse* response,
                        ::google::protobuf::Closure* done) {
    int64_t block_id = request->block_id();
    const std::string& databuf = request->databuf();
    int64_t offset = request->offset();
    int32_t packet_seq = request->packet_seq();

    if (!response->has_sequence_id()) {
        response->set_sequence_id(request->sequence_id());
        LOG(DEBUG, "[WriteBlock] dispatch [bid:%ld, seq:%d, offset:%ld, len:%lu] %lu\n",
           block_id, packet_seq, offset, databuf.size(), request->sequence_id());
        boost::function<void ()> task =
            boost::bind(&ChunkServerImpl::WriteBlock, this, controller, request, response, done);
        _thread_pool->AddTask(task);
        return;
    }

    LOG(INFO, "[WriteBlock] [bid:%ld, seq:%d, offset:%ld, len:%lu] %lu\n",
           block_id, packet_seq, offset, databuf.size(), request->sequence_id());

    if (request->chunkservers_size()) {
        // New request for next chunkserver
        WriteBlockRequest* next_request = new WriteBlockRequest(*request);
        next_request->clear_chunkservers();
        for (int i = 1; i < request->chunkservers_size(); i++) {
            next_request->add_chunkservers(request->chunkservers(i));
        }
        ChunkServer_Stub* stub = NULL;
        const std::string& next_server = request->chunkservers(0);
        _rpc_client->GetStub(next_server, &stub);
        WriteNext(next_server, stub, next_request, request, response, done);
    } else {
        const WriteBlockRequest* next_request = NULL;
        ChunkServer_Stub* stub = NULL;
        boost::function<void ()> callback =
            boost::bind(&ChunkServerImpl::WriteNextCallback,
                this, next_request, response, false, 0, "", request, done, stub);
        _thread_pool->AddTask(callback);
    }
}

void ChunkServerImpl::WriteNext(const std::string& next_server,
                                ChunkServer_Stub* stub,
                                const WriteBlockRequest* next_request,
                                const WriteBlockRequest* request,
                                WriteBlockResponse* response,
                                ::google::protobuf::Closure* done) {
    int64_t block_id = request->block_id();
    int32_t packet_seq = request->packet_seq();
    LOG(INFO, "Writeblock send [bid:%ld, seq:%d] to next %s\n",
        block_id, packet_seq, next_server.c_str());
    boost::function<void (const WriteBlockRequest*, WriteBlockResponse*, bool, int)> callback =
        boost::bind(&ChunkServerImpl::WriteNextCallback,
            this, _1, _2, _3, _4, next_server, request, done, stub);
    _rpc_client->AsyncRequest(stub, &ChunkServer_Stub::WriteBlock,
        next_request, response, callback, 5, 3);
}

void ChunkServerImpl::WriteNextCallback(const WriteBlockRequest* next_request,
                        WriteBlockResponse* response,
                        bool failed, int error,
                        const std::string& next_server,
                        const WriteBlockRequest* request,
                        ::google::protobuf::Closure* done,
                        ChunkServer_Stub* stub) {
    /// If RPC_ERROR_SEND_BUFFER_FULL retry send.
    if (failed && error == sofa::pbrpc::RPC_ERROR_SEND_BUFFER_FULL) {
        boost::function<void ()> callback = 
            boost::bind(&ChunkServerImpl::WriteNext, this, next_server,
                        stub, next_request, request, response, done);
        _thread_pool->DelayTask(10, callback);
        return;
    }

    int64_t block_id = request->block_id();
    const std::string& databuf = request->databuf();
    int64_t offset = request->offset();
    int32_t packet_seq = request->packet_seq();
    delete stub;
    delete next_request;
    if (failed || response->status() != 0) {
        if (!response->has_bad_chunkserver()) {
            response->set_bad_chunkserver("self address");
        }
        LOG(WARNING, "WriteNext %s fail: [bid:%ld, seq:%d, offset:%ld, len:%lu], "
                     "status= %d, error= %d\n",
            next_server.c_str(), block_id, packet_seq, offset, databuf.size(),
            response->status(), error);
        if (response->status() == 0) {
            response->set_status(error);
        }
        done->Run();
        return;
    } else {
        LOG(INFO, "Writeblock send [bid:%ld, seq:%d] to next done", block_id, packet_seq);
    }

    if (!response->has_status()) {
        response->set_status(0);
    }

    /// search;
    Block* block = _block_manager->FindBlock(block_id, true);
    if (!block) {
        LOG(WARNING, "Block not found: %ld\n", block_id);
        response->set_status(8404);
        done->Run();
        return;
    }

    if (!block->Write(packet_seq, offset, databuf.data(), databuf.size())) {
        LOG(WARNING, "Write offset[%ld] block_size[%ld] not in sliding window\n",
            offset, block->Size());
        block->DecRef();
        response->set_status(812);
        done->Run();
        return;
    }
    LOG(INFO, "WriteBlock done [bid:%ld, seq:%d, offset:%ld, len:%lu]\n",
        block_id, packet_seq, offset, databuf.size());
    if (request->is_last()) {
        block->SetSliceNum(packet_seq + 1);
    }

    // If complete, close block, and report only once(close block return true).
    if (block->IsComplete() && _block_manager->CloseBlock(block)) {
        LOG(INFO, "WriteBlock block finish [%ld:%ld]\n", block_id, block->Size());
        ReportFinish(block);
    }
    done->Run();
    block->DecRef();
    block = NULL;
}

void ChunkServerImpl::ReadBlock(::google::protobuf::RpcController* controller,
                        const ReadBlockRequest* request,
                        ReadBlockResponse* response,
                        ::google::protobuf::Closure* done) {
    if (!response->has_sequence_id()) {
        response->set_sequence_id(request->sequence_id());
        boost::function<void ()> task =
            boost::bind(&ChunkServerImpl::ReadBlock, this, controller, request, response, done);
        _thread_pool->AddTask(task);
        return;
    }

    int64_t block_id = request->block_id();
    int64_t offset = request->offset();
    int32_t read_len = request->read_len();
    int status = 0;

    Block* block = _block_manager->FindBlock(block_id, false);
    if (block == NULL) {
        status = 404;
        LOG(WARNING, "ReadBlock not found: %ld offset: %ld len: %d\n",
                block_id, offset, read_len);
    } else {
        char* buf = new char[read_len];
        int32_t len = block->Read(buf, read_len, offset);
        if (len >= 0) {
            response->mutable_databuf()->assign(buf, len);
            LOG(INFO, "ReadBlock: %ld offset: %ld len: %d return: %d",
                block_id, offset, read_len, len);
        } else {
            status = 882;
            LOG(WARNING, "ReadBlock fail: %ld offset: %ld len: %d\n",
                block_id, offset, read_len);
        }
        delete[] buf;
    }
    block->DecRef();
    response->set_status(status);
    done->Run();
}
void ChunkServerImpl::RemoveObsoleteBlocks(std::vector<int64_t> blocks) {
    for (size_t i = 0; i < blocks.size(); i++) {
        if (!_block_manager->RemoveBlock(blocks[i])) {
            LOG(WARNING, "Remove block fail: %ld\n", blocks[i]);
        }
    }
}
void ChunkServerImpl::AddNewReplica(std::vector<ReplicaInfo> new_replica_info) {
    for (size_t i = 0; i < new_replica_info.size(); i++) {
        int64_t block_id = new_replica_info[i].block_id();
        Block* block = _block_manager->FindBlock(block_id, false);
        if (block == NULL) {
            LOG(WARNING, "ReadBlock not found: %ld\n", block_id);
        } else {
            char* buf = new char[256 * 1024];
            ChunkServer_Stub* cur_chunkserver = NULL;
            if (!_rpc_client->GetStub(new_replica_info[i].chunkserver_address(0), &cur_chunkserver)) {
                LOG(WARNING, "Can't connect to chunkserver:%s\n",
                        new_replica_info[i].chunkserver_address(0).c_str());
                continue;
            }
            int32_t len;
            int64_t offset = 0;
            int64_t packet_seq = 0;
            while((len = block->Read(buf, 256 * 1024, offset)) != -1) {
                if (len >= 0) {
                    WriteBlockRequest request;
                    WriteBlockResponse response;
                    int64_t seq = common::timer::get_micros();
                    request.set_sequence_id(seq);
                    request.set_block_id(block_id);
                    request.set_databuf(buf, len);
                    request.set_offset(offset);
                    request.set_is_last(len == 0);
                    request.set_packet_seq(packet_seq);
                    request.set_is_append(false);
                    for (int j = 1; j < new_replica_info[i].chunkserver_address_size(); j++) {
                        request.add_chunkservers(new_replica_info[i].chunkserver_address(j));
                    }
                    if (!_rpc_client->SendRequest(cur_chunkserver,
                                &ChunkServer_Stub::WriteBlock, &request, &response, 5, 3)) {
                        LOG(WARNING, "AddNewReplica %ld rpc call fail\n", block_id);
                        break;
                    }
                    if (response.status() != 0) {
                        LOG(WARNING, "AddNewReplica %ld fail\n", block_id);
                        break;
                    }
                }
                if (len == 0) {
                    break;
                }
                offset += len;
                packet_seq++;
            }
            delete[] buf;
        }
        // send to nameserver finish
        FinishBlockRequest request;
        FinishBlockResponse response;
        request.set_sequence_id(0);
        request.set_block_id(block_id);
        if (!_rpc_client->SendRequest(_nameserver, &NameServer_Stub::FinishBlock,
                    &request, &response, 5, 3)) {
            LOG(WARNING, "Fail to report finish to nameserver: %ld\n", block_id);
        } else {
            if (response.status() == 0) {
                LOG(INFO, "Report add new replica finish to nameserver: %ld\n", block_id);
            } else {
                LOG(WARNING, "Report add new replica finish to nameserver fail: %ld\n", block_id);
            }
        }
    }
}

} // namespace bfs


/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
