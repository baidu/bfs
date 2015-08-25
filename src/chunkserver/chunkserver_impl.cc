// Copyright (c) 2014, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#include "chunkserver_impl.h"

#include <errno.h>
#include <fcntl.h>
#include <string.h>
#include <sys/stat.h>

#include <boost/bind.hpp>
#include <gflags/gflags.h>

#include <leveldb/db.h>
#include <leveldb/cache.h>

#include "common/mutex.h"
#include "common/atomic.h"
#include "common/counter.h"
#include "common/mutex.h"
#include "common/util.h"
#include "common/timer.h"
#include "common/sliding_window.h"
#include "common/string_util.h"
#include "file_cache.h"
#include "proto/nameserver.pb.h"
#include "rpc/rpc_client.h"

// Avoid conflict, we define LOG...
#include "common/logging.h"

DECLARE_string(block_store_path);
DECLARE_string(nameserver);
DECLARE_string(nameserver_port);
DECLARE_string(chunkserver_port);
DECLARE_int32(heartbeat_interval);
DECLARE_int32(blockreport_interval);
DECLARE_int32(blockreport_size);
DECLARE_int32(write_buf_size);
DECLARE_int32(chunkserver_work_thread_num);
DECLARE_int32(chunkserver_read_thread_num);
DECLARE_int32(chunkserver_write_thread_num);
DECLARE_int32(chunkserver_file_cache_size);
DECLARE_int32(chunkserver_max_pending_buffers);
DECLARE_string(nexus_root_path);
DECLARE_string(master_path);
DECLARE_string(nexus_servers);

namespace bfs {

common::Counter g_block_buffers;
common::Counter g_buffers_new;
common::Counter g_buffers_delete;
common::Counter g_blocks;
common::Counter g_writing_bytes;
common::Counter g_find_ops;
common::Counter g_read_ops;
common::Counter g_write_ops;
common::Counter g_write_bytes;
common::Counter g_rpc_delay;
common::Counter g_rpc_delay_all;
common::Counter g_rpc_count;
common::Counter g_data_size;

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
    int64_t version;
    BlockMeta()
      : block_id(0), block_size(0), checksum(0), version(-1) {
    }
};

/// Data block
class Block {
public:
    Block(const BlockMeta& meta, const std::string& store_path, ThreadPool* thread_pool,
          FileCache* file_cache) :
      _thread_pool(thread_pool), _meta(meta),
      _last_seq(-1), _slice_num(-1), _blockbuf(NULL), _buflen(0),
      _bufdatalen(0), _disk_writing(false),
      _disk_file_size(meta.block_size), _file_desc(-1), _refs(0),
      _recv_window(NULL), _finished(false), _deleted(false),
      _file_cache(file_cache) {
        assert(_meta.block_id < (1L<<40));
        g_data_size.Add(meta.block_size);
        char file_path[16];
        int len = snprintf(file_path, sizeof(file_path), "/%03ld/%010ld",
            _meta.block_id % 1000, _meta.block_id / 1000);
        assert (len == 15);
        _disk_file = store_path + file_path;
        g_blocks.Inc();
        _recv_window = new common::SlidingWindow<Buffer>(100,
                       boost::bind(&Block::WriteCallback, this, _1, _2));
    }
    ~Block() {
        if (_bufdatalen > 0) {
            if (!_deleted) {
                LOG(WARNING, "Data lost, %d bytes in #%ld %s",
                    _bufdatalen, _meta.block_id, _disk_file.c_str());
            }
        }
        if (_blockbuf) {
            delete[] _blockbuf;
            g_block_buffers.Dec();
            g_buffers_delete.Inc();
            _blockbuf = NULL;
        }
        _buflen = 0;
        _bufdatalen = 0;

        LOG(INFO, "Release #%ld _block_buf_list size= %lu",
            _meta.block_id, _block_buf_list.size());
        for (uint32_t i = 0; i < _block_buf_list.size(); i++) {
            const char* buf = _block_buf_list[0].first;
            int len = _block_buf_list[0].second;
            if (!_deleted) {
                LOG(WARNING, "Data lost, %d bytes in %s, #%ld _block_buf_list",
                    len, _disk_file.c_str(), _meta.block_id);
            } else {
                LOG(INFO, "Release _block_buf_list %d for #%ld ", len, _meta.block_id);
            }
            delete[] buf;
            g_block_buffers.Dec();
            g_buffers_delete.Inc();
        }
        _block_buf_list.clear();

        if (_file_desc >= 0) {
            close(_file_desc);
            _file_desc = -1;
        }
        if (_recv_window) {
            if (_recv_window->Size()) {
                LOG(INFO, "#%ld recv_window fragments: %d\n",  
                    _meta.block_id, _recv_window->Size());
                std::vector<std::pair<int32_t,Buffer> > frags;
                _recv_window->GetFragments(&frags);
                for (uint32_t i = 0; i < frags.size(); i++) {
                    delete[] frags[i].second.data_;
                }
            }
            delete _recv_window;
        }
        LOG(INFO, "Block #%ld deleted\n", _meta.block_id);
        g_blocks.Dec();
        g_data_size.Sub(_meta.block_size);
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
    int64_t DiskUsed() {
        return _disk_file_size;
    }
    void SetDeleted() {
        _deleted = true;
    }
    /// Open corresponding file for write.
    bool OpenForWrite() {
        _mu.AssertHeld();
        if (_file_desc >= 0) return true;
        /// Unlock for disk operating.
        _mu.Unlock();
        std::string dir = _disk_file.substr(0, _disk_file.rfind('/'));
        // Mkdir dir for data block, ignore error, may already exist.
        mkdir(dir.c_str(), 0755);
        int fd  = open(_disk_file.c_str(), O_RDWR | O_CREAT | O_TRUNC, S_IRUSR);
        if (fd < 0) {
            LOG(WARNING, "Open block #%ld %s fail: %s",
                _meta.block_id, _disk_file.c_str(), strerror(errno));
            return false;
        }
        _mu.Lock("Block::OpenForWrite");
        _file_desc = fd;
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
    /// Read operation.
    int32_t Read(char* buf, int32_t len, int64_t offset) {
        MutexLock lock(&_mu, "Block::Read", 1000);
        if (offset > _meta.block_size) {
            return -1;
        }

        /// Read from disk
        int readlen = 0;
        while (offset + readlen < _disk_file_size) {
            int pread_len = std::min(len - readlen,
                                     static_cast<int>(_disk_file_size - offset - readlen));
            _mu.Unlock();
            int ret = _file_cache->ReadFile(_disk_file, 
                            buf + readlen, pread_len, offset + readlen);
            assert(ret == pread_len);
            readlen += ret;
            _mu.Lock("Block::Read relock", 1000);
            if (readlen >= len) return readlen;
            // If disk_file_size change, read again.
        }
        // Read from block_buf_list
        int mem_offset = offset + readlen - _disk_file_size;
        uint32_t buf_id = mem_offset / FLAGS_write_buf_size;
        mem_offset %= FLAGS_write_buf_size;
        while (buf_id < _block_buf_list.size()) {
            const char* block_buf = _block_buf_list[buf_id].first;
            int buf_len = _block_buf_list[buf_id].second;
            int mlen = std::min(len - readlen, buf_len - mem_offset);
            memcpy(buf + readlen, block_buf + mem_offset, mlen);
            readlen += mlen;
            mem_offset = 0;
            buf_id ++;
            if (readlen >= len) return readlen;
        }
        // Read from block buf
        assert (mem_offset >= 0);
        if (mem_offset < _bufdatalen) {
            int mlen = std::min(_bufdatalen - mem_offset, len - readlen);
            memcpy(buf + readlen, _blockbuf + mem_offset, mlen);
            readlen += mlen;
        }

        return readlen;
    }
    /// Write operation.
    bool Write(int32_t seq, int64_t offset, const char* data,
               int32_t len, int64_t* add_use = NULL) {
        if (offset < _meta.block_size) {
            assert (offset + len <= _meta.block_size);
            LOG(WARNING, "Write a finish block #%ld size %ld, seq: %d, offset: %ld",
                _meta.block_id, _meta.block_size, seq, offset);
            return true;
        }
        char* buf = NULL;
        if (len) {
            buf = new char[len];
            memcpy(buf, data, len);
            g_writing_bytes.Add(len);
        }
        int64_t add_start = common::timer::get_micros();
        int ret = _recv_window->Add(seq, Buffer(buf, len));
        if (add_use) *add_use = common::timer::get_micros() - add_start;
        if (ret != 0) {
            delete[] buf;
            g_writing_bytes.Sub(len);
            if (ret < 0) {
                LOG(WARNING, "Write block #%ld seq: %d, offset: %ld, block_size: %ld"
                             " not in sliding window\n",
                    _meta.block_id, seq, offset, _meta.block_size);
                return false;
            }
        }
        return true;
    }
    /// Flush block to disk.
    bool Close() {
        MutexLock lock(&_mu, "Block::Close", 1000);
        if (_finished) {
            return false;
        }

        LOG(INFO, "Block #%ld flush to %s", _meta.block_id, _disk_file.c_str());
        if (_bufdatalen) {
            _block_buf_list.push_back(std::make_pair(_blockbuf, _bufdatalen));
            _blockbuf = NULL;
            _bufdatalen = 0;
        }
        _finished = true;
        // DiskWrite will close _file_desc asynchronously.
        if (!_disk_writing) {
            this->AddRef();
            _thread_pool->AddTask(boost::bind(&Block::DiskWrite, this));
        }
        return true;
    }
    void AddRef() {
        common::atomic_inc(&_refs);
        assert (_refs > 0);
    }
    void DecRef() {
        if (common::atomic_add(&_refs, -1) == 1) {
            assert(_refs == 0);
            delete this;
        }
    }
private:
    /// Invoke by slidingwindow, when next buffer arrive.
    void WriteCallback(int32_t seq, Buffer buffer) {
        Append(seq, buffer.data_, buffer.len_);
        delete[] buffer.data_;
        g_writing_bytes.Sub(buffer.len_);
    }
    void DiskWrite() {
        MutexLock lock(&_mu, "Block::DiskWrite", 1000);
        if (!_disk_writing && !_deleted) {
            _disk_writing = true;
            while (!_block_buf_list.empty() && !_deleted) {
                if (!OpenForWrite())assert(0);
                const char* buf = _block_buf_list[0].first;
                int len = _block_buf_list[0].second;

                // Unlock when disk write
                _mu.Unlock();
                int wlen = 0;
                while (wlen < len) {
                    int w = write(_file_desc, buf + wlen, len - wlen);
                    if (w < 0) {
                        LOG(WARNING, "IOEroro write #%ld %s return %s",
                            _meta.block_id, _disk_file.c_str(), strerror(errno));
                        assert(0);
                        break;
                    }
                    wlen += w;
                }
                delete[] buf;
                g_block_buffers.Dec();
                g_buffers_delete.Inc();
                // Re-Lock for commit
                _mu.Lock("Block::DiskWrite ReLock", 1000);
                _disk_file_size += len;
                _block_buf_list.erase(_block_buf_list.begin());
            }
            _disk_writing = false;
        }
        if (!_disk_writing && (_finished || _deleted)) {
            if (_file_desc != -1) {
                int ret = close(_file_desc);
                LOG(INFO, "DiskWrite close file %s", _disk_file.c_str());
                assert(ret == 0);
            }
            _file_desc = -1;
        }
        this->DecRef();
    }
    /// Append to block buffer
    void Append(int32_t seq, const char*buf, int32_t len) {
        MutexLock lock(&_mu, "BlockAppend", 1000);
        if (_blockbuf == NULL) {
            _buflen = FLAGS_write_buf_size;
            _blockbuf = new char[_buflen];
            g_block_buffers.Inc();
            g_buffers_new.Inc();
        }
        int ap_len = len;
        while (_bufdatalen + ap_len > _buflen) {
            int wlen = _buflen - _bufdatalen;
            memcpy(_blockbuf + _bufdatalen, buf, wlen);
            _block_buf_list.push_back(std::make_pair(_blockbuf, FLAGS_write_buf_size));
            this->AddRef();
            _thread_pool->AddTask(boost::bind(&Block::DiskWrite, this));
            
            _blockbuf = new char[_buflen];
            g_block_buffers.Inc();
            g_buffers_new.Inc();
            _bufdatalen = 0;
            buf += wlen;
            ap_len -= wlen;
        }
        if (ap_len) {
            memcpy(_blockbuf + _bufdatalen, buf, ap_len);
            _bufdatalen += ap_len;
        }
        _meta.block_size += len;
        g_data_size.Add(len);
        _last_seq = seq;
    }
private:
    enum Type {
        InDisk,
        InMem,
    };
    ThreadPool* _thread_pool;
    BlockMeta   _meta;
    int32_t     _last_seq;
    int32_t     _slice_num;
    char*       _blockbuf;
    int32_t     _buflen;
    int32_t     _bufdatalen;
    std::vector<std::pair<const char*,int> > _block_buf_list;
    bool        _disk_writing;
    std::string _disk_file;
    int32_t     _disk_file_size;
    int         _file_desc; ///< disk file fd
    volatile int _refs;
    Mutex       _mu;
    common::SlidingWindow<Buffer>* _recv_window;
    bool        _finished;
    bool        _deleted;

    FileCache*  _file_cache;
};

class BlockManager {
public:
    BlockManager(ThreadPool* thread_pool, const std::string& store_path)
        :_thread_pool(thread_pool),
         _metadb(NULL) {
         ParseStorePath(store_path);
         _file_cache = new FileCache(FLAGS_chunkserver_file_cache_size);
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
    void ParseStorePath(const std::string& store_path) {
         common::SplitString(store_path, ",", &_store_path_list);
         for (uint32_t i = 0 ;i < _store_path_list.size(); ++i) {
            std::string& store_path = _store_path_list[i];
            store_path = common::TrimString(store_path, " ");
            if (store_path.empty() || store_path[store_path.size() - 1] != '/') {
                store_path += "/";
            }
            LOG(INFO, "Use store path: %s", store_path.c_str());
         }
         std::sort(_store_path_list.begin(), _store_path_list.end());
         std::vector<std::string>::iterator it
            = std::unique(_store_path_list.begin(), _store_path_list.end());
         _store_path_list.resize(std::distance(_store_path_list.begin(), it));
         LOG(INFO, "%lu store path used.", _store_path_list.size());
         assert(_store_path_list.size() > 0);
    }
    const std::string& GetStorePath(int64_t block_id) {
        return _store_path_list[block_id % _store_path_list.size()];
    }
    bool LoadStorage() {
        MutexLock lock(&_mu);
        leveldb::Options options;
        options.create_if_missing = true;
        leveldb::Status s = leveldb::DB::Open(options, _store_path_list[0] + "meta/", &_metadb);
        if (!s.ok()) {
            LOG(WARNING, "Load blocks fail");
            return false;
        }
        int block_num = 0;
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
            Block* block = new Block(meta, GetStorePath(block_id), _thread_pool, _file_cache);
            block->AddRef();
            _block_map[block_id] = block;
            block_num ++;
        }
        delete it;
        LOG(INFO, "Load %ld blocks\n", block_num);
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
    Block* FindBlock(int64_t block_id, bool create_if_missing, int64_t* sync_time = NULL) {
        Block* block = NULL;
        {
            MutexLock lock(&_mu, "BlockManger::Find", 1000);
            g_find_ops.Inc();
            BlockMap::iterator it = _block_map.find(block_id);
            if (it != _block_map.end()) {
                block = it->second;
            } else if (create_if_missing) {
                BlockMeta meta;
                meta.block_id = block_id;
                meta.version = 0;
                block = new Block(meta, GetStorePath(block_id), _thread_pool, _file_cache);
                // for block_map
                block->AddRef();
                _block_map[block_id] = block;
                // Unlock for write meta & sync
                _mu.Unlock();
                if (!SyncBlockMeta(meta, sync_time)) {
                    delete block;
                    block = NULL;
                }
                _mu.Lock();
                if (!block) {
                    _block_map.erase(block_id);
                }
            } else {
                // not found
            }
        }
        // for user
        if (block) {
            block->AddRef();
        }
        return block;
    }
    bool SyncBlockMeta(const BlockMeta& meta, int64_t* sync_time) {
        char idstr[64];
        snprintf(idstr, sizeof(idstr), "%13ld", meta.block_id);

        leveldb::WriteOptions options;
        // options.sync = true;
        int64_t time_start = common::timer::get_micros();
        leveldb::Status s = _metadb->Put(options, idstr,
            leveldb::Slice(reinterpret_cast<const char*>(&meta),sizeof(meta)));
        int64_t time_use = common::timer::get_micros() - time_start;
        if (sync_time) *sync_time = time_use;
        if (!s.ok()) {
            Log(WARNING, "Write to meta fail:%s\n", idstr);
            return false;
        }
        return true;
    }
    bool CloseBlock(Block* block) {
        if (!block->Close()) {
            return false;
        }
        
        // Update meta
        BlockMeta meta = block->GetMeta();
        return SyncBlockMeta(meta, NULL);
    }
    bool RemoveBlock(int64_t block_id) {
        Block* block = NULL;
        {
            MutexLock lock(&_mu, "BlockManager::RemoveBlock", 1000);
            BlockMap::iterator it = _block_map.find(block_id);
            if (it == _block_map.end()) {
                LOG(INFO, "Try to remove block that does not exist: #%ld ", block_id);
                return false;
            }
            block = it->second;
            block->AddRef();
        }

        int64_t du = block->DiskUsed();
        block->SetDeleted();
        std::string file_path = block->GetFilePath();
        int ret = remove(file_path.c_str());
        if (ret != 0 && (errno !=2 || du > 0)) {
            LOG(WARNING, "Remove #%ld disk file %s %ld bytes fails: %d (%s)",
                block_id, file_path.c_str(), du, errno, strerror(errno));
        } else {
            LOG(INFO, "Remove #%ld disk file done: %s\n", 
                block_id, file_path.c_str());
        }

        char dir_name[5];
        snprintf(dir_name, sizeof(dir_name), "/%03ld", block_id % 1000);
        // Rmdir, ignore error when not empty.
        // rmdir((GetStorePath(block_id) + dir_name).c_str());
        char idstr[14];
        snprintf(idstr, sizeof(idstr), "%13ld", block_id);

        leveldb::Status s = _metadb->Delete(leveldb::WriteOptions(), idstr);
        if (s.ok()) {
            LOG(INFO, "Remove #%ld meta info done", block_id);
            {
                MutexLock lock(&_mu, "BlockManager::RemoveBlock erase", 1000);
                _block_map.erase(block_id);
            }
            block->DecRef();
            ret = true;
        } else {
            LOG(WARNING, "Remove #%ld meta info fails: %s", block_id, s.ToString().c_str());
            ret = false;
        }
        block->DecRef();
        return ret;
    }
private:
    ThreadPool* _thread_pool;
    std::vector<std::string> _store_path_list;
    typedef std::map<int64_t, Block*> BlockMap;
    BlockMap  _block_map;
    leveldb::DB* _metadb;
    FileCache* _file_cache;
    Mutex   _mu;
};


static void OnMasterChange(const galaxy::ins::sdk::WatchParam& param,
        galaxy::ins::sdk::SDKError error) {
    ChunkServerImpl* cs = static_cast<ChunkServerImpl*>(param.context);
    cs->HandleMasterChange(param.value);
}

void ChunkServerImpl::HandleMasterChange(const std::string& new_master_endpoint) {
    MutexLock lock(&_master_mutex);
    if (new_master_endpoint.empty()) {
        LOG(WARNING, "master endpoint is deleted from nexus");
    }
    if (new_master_endpoint != _master_nameserver_addr) {
        LOG(INFO, "master change to %s", new_master_endpoint.c_str());
        _master_nameserver_addr = new_master_endpoint;
        if (_nameserver) {
            delete _nameserver;
        }
        if (!_rpc_client->GetStub(_master_nameserver_addr, &_nameserver)) {
            LOG(WARNING, "connect master %s failed", _master_nameserver_addr.c_str());
            return;
        }
    }
    std::string master_path_key = FLAGS_nexus_root_path + FLAGS_master_path;
    galaxy::ins::sdk::SDKError err;
    bool ret = _nexus->Watch(master_path_key, &OnMasterChange, this, &err);
    if (!ret) {
        LOG(FATAL, "fail to watch again on nexus, err_code: %d", err);
    } else {
        LOG(INFO, "watch master again success");
    }
}

bool ChunkServerImpl::RegistToMaster() {
    MutexLock lock(&_master_mutex);
    std::string master_path_key =  FLAGS_nexus_root_path + FLAGS_master_path;
    galaxy::ins::sdk::SDKError err;
    bool ret = _nexus->Get(master_path_key, &_master_nameserver_addr, &err);
    if (ret) {
        LOG(INFO, "master is: %s", _master_nameserver_addr.c_str());
    } else {
        LOG(FATAL, "fail to get master address from nexus, error_code: %d", err);
        abort();
    }
    ret = _nexus->Watch(master_path_key, &OnMasterChange, this, &err);
    if (ret) {
        LOG(INFO, "watch master success");
    } else {
        LOG(FATAL, "fail to watch on nexus, err_code: %d", err);
    }

    if (!_rpc_client->GetStub(_master_nameserver_addr, &_nameserver)) {
        LOG(WARNING, "connect master %s failed", _master_nameserver_addr.c_str());
        return false;
    }
    return true;
}

ChunkServerImpl::ChunkServerImpl()
    : _quit(false), _chunkserver_id(0), _namespace_version(0) {
    _data_server_addr = common::util::GetLocalHostName() + ":" + FLAGS_chunkserver_port;
    _work_thread_pool = new ThreadPool(FLAGS_chunkserver_work_thread_num);
    _read_thread_pool = new ThreadPool(FLAGS_chunkserver_read_thread_num);
    _write_thread_pool = new ThreadPool(FLAGS_chunkserver_write_thread_num);
    _block_manager = new BlockManager(_write_thread_pool, FLAGS_block_store_path);
    bool s_ret = _block_manager->LoadStorage();
    assert(s_ret == true);
    _rpc_client = new RpcClient();
    _nexus = new galaxy::ins::sdk::InsSDK(FLAGS_nexus_servers);
    s_ret = RegistToMaster();
    assert(s_ret);
    _work_thread_pool->AddTask(boost::bind(&ChunkServerImpl::LogStatus, this));
    int ret = pthread_create(&_routine_thread, NULL, RoutineWrapper, this);
    assert(ret == 0);
}

ChunkServerImpl::~ChunkServerImpl() {
    /// TODO: is destructor necessary?
    _quit = true;
    pthread_join(_routine_thread, NULL);
    _work_thread_pool->Stop(true);
    _read_thread_pool->Stop(true);
    _write_thread_pool->Stop(true);
    delete _work_thread_pool;
    delete _read_thread_pool;
    delete _write_thread_pool;
    delete _block_manager;
    delete _rpc_client;
    delete _nexus;
}

void ChunkServerImpl::LogStatus() {
    int64_t rpc_count = g_rpc_count.Clear();
    int64_t rpc_delay = 0;
    int64_t delay_all = 0;
    if (rpc_count) {
        rpc_delay = g_rpc_delay.Clear() / rpc_count / 1000;
        delay_all = g_rpc_delay_all.Clear() / rpc_count / 1000;
    }
    LOG(INFO, "[Status] blocks %ld buffers %ld data %sB, "
              "find %ld read %ld write %ld %.2f MB, rpc_delay %ld %ld",
        g_blocks.Get(), g_block_buffers.Get(),
        common::HumanReadableString(g_data_size.Get()).c_str(),
        g_find_ops.Clear()/5, g_read_ops.Clear()/5,
        g_write_ops.Clear()/5, g_write_bytes.Clear() / 1024 / 1024 / 5.0,
        rpc_delay, delay_all);
    _work_thread_pool->DelayTask(5000, boost::bind(&ChunkServerImpl::LogStatus, this));
}

void* ChunkServerImpl::RoutineWrapper(void* arg) {
    reinterpret_cast<ChunkServerImpl*>(arg)->Routine();
    return NULL;
}

void ChunkServerImpl::Routine() {
    static int64_t ticks = 0;
    int64_t next_report = -1;
    size_t next_report_offset = 0;
    std::vector<BlockMeta> blocks;
    while (!_quit) {
        // heartbeat
        if (ticks % FLAGS_heartbeat_interval == 0) {
            HeartBeatRequest request;
            request.set_chunkserver_id(_chunkserver_id);
            request.set_data_server_addr(_data_server_addr);
            request.set_namespace_version(_namespace_version);
            request.set_block_num(g_blocks.Get());
            request.set_data_size(g_data_size.Get());
            HeartBeatResponse response;
            NameServer_Stub* stub = NULL;
            {
                MutexLock lock(&_master_mutex);
                _rpc_client->GetStub(_master_nameserver_addr, &stub);
            }
            if (!_rpc_client->SendRequest(_nameserver, &NameServer_Stub::HeartBeat,
                    &request, &response, 15, 1)) {
                LOG(WARNING, "Heat beat fail\n");
            } else if (_namespace_version != response.namespace_version()) {
                LOG(INFO, "Connect to nameserver, new chunkserver_id: %d\n",
                    response.chunkserver_id());
                _namespace_version = response.namespace_version();
                _chunkserver_id = response.chunkserver_id();
                next_report = ticks;
                next_report_offset = 0;
            }
            delete stub;
        }
        // block report
        if (ticks == next_report) {
            BlockReportRequest request;
            request.set_chunkserver_id(_chunkserver_id);
            request.set_chunkserver_addr(_data_server_addr);
            request.set_namespace_version(_namespace_version);

            if (next_report_offset == 0) {
                blocks.clear();
                _block_manager->ListBlocks(&blocks);
                std::vector<BlockMeta>(blocks).swap(blocks);
            }
            size_t blocks_num = blocks.size();
            size_t last_block = ticks ?
                next_report_offset + FLAGS_blockreport_size : blocks_num;
            size_t i = next_report_offset;
            for (; i < last_block && i < blocks_num; i++) {
                ReportBlockInfo* info = request.add_blocks();
                info->set_block_id(blocks[i].block_id);
                info->set_block_size(blocks[i].block_size);
                info->set_version(0);
            }
            next_report_offset = i;
            if (next_report_offset >= blocks_num) {
                next_report_offset = 0;
                request.set_is_complete(true);
            } else {
                request.set_is_complete(false);
            }
            BlockReportResponse response;
            NameServer_Stub* stub = NULL;
            {
                MutexLock lock(&_master_mutex);
                _rpc_client->GetStub(_master_nameserver_addr, &stub);
            }
            if (!_rpc_client->SendRequest(_nameserver, &NameServer_Stub::BlockReport,
                    &request, &response, 20, 3)) {
                LOG(WARNING, "Block reprot fail\n");
                next_report += 60;  // retry
            } else {
                if (_namespace_version != response.namespace_version()) {
                    next_report_offset = 0;
                    next_report = ticks + 1;
                } else {
                    next_report += FLAGS_blockreport_interval;
                    //deal with obsolete blocks
                    std::vector<int64_t> obsolete_blocks;
                    for (int i = 0; i < response.obsolete_blocks_size(); i++) {
                        obsolete_blocks.push_back(response.obsolete_blocks(i));
                    }
                    if (!obsolete_blocks.empty()) {
                        boost::function<void ()> task =
                            boost::bind(&ChunkServerImpl::RemoveObsoleteBlocks,
                                    this, obsolete_blocks);
                        _write_thread_pool->AddTask(task);
                    }

                    std::vector<ReplicaInfo> new_replica_info;
                    for (int i = 0; i < response.new_replicas_size(); i++) {
                        new_replica_info.push_back(response.new_replicas(i));
                    }
                    if (!new_replica_info.empty()) {
                        boost::function<void ()> new_replica_task =
                            boost::bind(&ChunkServerImpl::PullNewBlocks,
                                    this, new_replica_info);
                        _write_thread_pool->AddTask(new_replica_task);
                    }
                }
            }
            delete stub;
        }
        ++ ticks;
        sleep(1);
    }
}

bool ChunkServerImpl::ReportFinish(Block* block) {
    BlockReportRequest request;
    request.set_chunkserver_id(_chunkserver_id);
    request.set_chunkserver_addr(_data_server_addr);
    request.set_namespace_version(_namespace_version);
    request.set_is_complete(false);

    ReportBlockInfo* info = request.add_blocks();
    info->set_block_id(block->Id());
    info->set_block_size(block->Size());
    info->set_version(0);
    BlockReportResponse response;
    NameServer_Stub* stub = NULL;
    {
        MutexLock lock(&_master_mutex);
        _rpc_client->GetStub(_master_nameserver_addr, &stub);
    }
    if (!_rpc_client->SendRequest(stub, &NameServer_Stub::BlockReport,
            &request, &response, 20, 3)) {
        LOG(WARNING, "Reprot finish fail: %ld\n", block->Id());
        delete stub;
        return false;
    }

    delete stub;
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
        LOG(DEBUG, "[WriteBlock] dispatch #%ld seq:%d, offset:%ld, len:%lu] %lu\n",
           block_id, packet_seq, offset, databuf.size(), request->sequence_id());
        response->add_timestamp(common::timer::get_micros());
        boost::function<void ()> task =
            boost::bind(&ChunkServerImpl::WriteBlock, this, controller, request, response, done);
        _work_thread_pool->AddTask(task);
        return;
    }

    response->add_timestamp(common::timer::get_micros());
    LOG(INFO, "[WriteBlock] #%ld seq:%d, offset:%ld, len:%lu",
           block_id, packet_seq, offset, databuf.size());

    if (request->chunkservers_size()) {
        // New request for next chunkserver
        WriteBlockRequest* next_request = new WriteBlockRequest(*request);
        WriteBlockResponse* next_response = new WriteBlockResponse();
        next_request->clear_chunkservers();
        for (int i = 1; i < request->chunkservers_size(); i++) {
            next_request->add_chunkservers(request->chunkservers(i));
        }
        ChunkServer_Stub* stub = NULL;
        const std::string& next_server = request->chunkservers(0);
        _rpc_client->GetStub(next_server, &stub);
        WriteNext(next_server, stub, next_request, next_response, request, response, done);
    } else {
        boost::function<void ()> callback =
            boost::bind(&ChunkServerImpl::LocalWriteBlock, this, request, response, done);
        _work_thread_pool->AddTask(callback);
    }
}

void ChunkServerImpl::WriteNext(const std::string& next_server,
                                ChunkServer_Stub* stub,
                                const WriteBlockRequest* next_request,
                                WriteBlockResponse* next_response,
                                const WriteBlockRequest* request,
                                WriteBlockResponse* response,
                                ::google::protobuf::Closure* done) {
    int64_t block_id = request->block_id();
    int32_t packet_seq = request->packet_seq();
    LOG(INFO, "[WriteBlock] send #%ld seq:%d to next %s\n",
        block_id, packet_seq, next_server.c_str());
    boost::function<void (const WriteBlockRequest*, WriteBlockResponse*, bool, int)> callback =
        boost::bind(&ChunkServerImpl::WriteNextCallback,
            this, _1, _2, _3, _4, next_server, std::make_pair(request, response), done, stub);
    _rpc_client->AsyncRequest(stub, &ChunkServer_Stub::WriteBlock,
        next_request, next_response, callback, 30, 3);
}

void ChunkServerImpl::WriteNextCallback(const WriteBlockRequest* next_request,
                        WriteBlockResponse* next_response,
                        bool failed, int error,
                        const std::string& next_server,
                        std::pair<const WriteBlockRequest*, WriteBlockResponse*> origin,
                        ::google::protobuf::Closure* done,
                        ChunkServer_Stub* stub) {
    const WriteBlockRequest* request = origin.first;
    WriteBlockResponse* response = origin.second;
    /// If RPC_ERROR_SEND_BUFFER_FULL retry send.
    if (failed && error == sofa::pbrpc::RPC_ERROR_SEND_BUFFER_FULL) {
        boost::function<void ()> callback = 
            boost::bind(&ChunkServerImpl::WriteNext, this, next_server,
                        stub, next_request, next_response, request, response, done);
        _work_thread_pool->DelayTask(10, callback);
        return;
    }
    delete next_request;
    delete stub;

    int64_t block_id = request->block_id();
    const std::string& databuf = request->databuf();
    int64_t offset = request->offset();
    int32_t packet_seq = request->packet_seq();
    if (failed || next_response->status() != 0) {
        LOG(WARNING, "[WriteBlock] WriteNext %s fail: #%ld seq:%d, offset:%ld, len:%lu], "
                     "status= %d, error= %d\n",
            next_server.c_str(), block_id, packet_seq, offset, databuf.size(),
            next_response->status(), error);
        if (next_response->status() == 0) {
            response->set_status(error);
        } else {
            response->set_status(next_response->status());
        }
        delete next_response;
        done->Run();
        return;
    } else {
        LOG(INFO, "[Writeblock] send #%ld seq:%d to next done", block_id, packet_seq);
        delete next_response;
    }
    
    boost::function<void ()> callback =
        boost::bind(&ChunkServerImpl::LocalWriteBlock, this, request, response, done);
    _work_thread_pool->AddTask(callback);
}

void ChunkServerImpl::LocalWriteBlock(const WriteBlockRequest* request,
                        WriteBlockResponse* response,
                        ::google::protobuf::Closure* done) {
    /// Flow control
    if (g_block_buffers.Get() > FLAGS_chunkserver_max_pending_buffers) {
        _write_thread_pool->DelayTask(10,
            boost::bind(&ChunkServerImpl::LocalWriteBlock, this, request, response, done));
        return;
    }

    int64_t block_id = request->block_id();
    const std::string& databuf = request->databuf();
    int64_t offset = request->offset();
    int32_t packet_seq = request->packet_seq();

    if (!response->has_status()) {
        response->set_status(0);
    }

    int64_t find_start = common::timer::get_micros();
    /// search;
    int64_t sync_time = 0;
    Block* block = _block_manager->FindBlock(block_id, true, &sync_time);
    if (!block) {
        LOG(WARNING, "[WriteBlock] Block not found: #%ld ", block_id);
        response->set_status(8404);
        done->Run();
        return;
    }

    int64_t add_used = 0;
    int64_t write_start = common::timer::get_micros();
    if (!block->Write(packet_seq, offset, databuf.data(), databuf.size(), &add_used)) {
        block->DecRef();
        response->set_status(812);
        done->Run();
        return;
    }
    g_write_bytes.Add(databuf.size());
    int64_t write_end = common::timer::get_micros();
    if (request->is_last()) {
        block->SetSliceNum(packet_seq + 1);
    }

    // If complete, close block, and report only once(close block return true).
    int64_t report_start = write_end;
    if (block->IsComplete() && _block_manager->CloseBlock(block)) {
        LOG(INFO, "[WriteBlock] block finish #%ld size:%ld", block_id, block->Size());
        report_start = common::timer::get_micros();
        ReportFinish(block);
    }

    int64_t time_end = common::timer::get_micros();
    LOG(INFO, "[WriteBlock] done #%ld seq:%d, offset:%ld, len:%lu "
              "use %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld ms",
        block_id, packet_seq, offset, databuf.size(),
        (response->timestamp(0) - request->sequence_id()) / 1000, // recv
        (response->timestamp(1) - response->timestamp(0)) / 1000, // dispatch time
        (find_start - response->timestamp(1)) / 1000, // async time
        (write_start - find_start - sync_time) / 1000,  // find time
        sync_time / 1000, // create sync time
        add_used / 1000, // sliding window add
        (write_end - write_start) / 1000,    // write time
        (report_start - write_end) / 1000, // close time
        (time_end - report_start) / 1000, // report time
        (time_end - response->timestamp(0)) / 1000); // total time
    g_rpc_delay.Add(response->timestamp(0) - request->sequence_id());
    g_rpc_delay_all.Add(time_end - request->sequence_id());
    g_rpc_count.Inc();
    g_write_ops.Inc();
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
        response->add_timestamp(common::timer::get_micros());
        boost::function<void ()> task =
            boost::bind(&ChunkServerImpl::ReadBlock, this, controller, request, response, done);
        _read_thread_pool->AddTask(task);
        return;
    }

    int64_t block_id = request->block_id();
    int64_t offset = request->offset();
    int32_t read_len = request->read_len();
    int status = 0;

    int64_t find_start = common::timer::get_micros();
    Block* block = _block_manager->FindBlock(block_id, false);
    if (block == NULL) {
        status = 404;
        LOG(WARNING, "ReadBlock not found: #%ld offset: %ld len: %d\n",
                block_id, offset, read_len);
    } else {
        int64_t read_start = common::timer::get_micros();
        char* buf = new char[read_len];
        int32_t len = block->Read(buf, read_len, offset);
        int64_t read_end = common::timer::get_micros();
        if (len >= 0) {
            response->mutable_databuf()->assign(buf, len);
            LOG(INFO, "ReadBlock #%ld offset: %ld len: %d return: %d "
                      "use %ld %ld %ld %ld %ld",
                block_id, offset, read_len, len,
                (response->timestamp(0) - request->sequence_id()) / 1000, // rpc time
                (find_start - response->timestamp(0)) / 1000,   // dispatch time
                (read_start - find_start) / 1000, // find time
                (read_end - read_start) / 1000,  // read time
                (read_end - response->timestamp(0)) / 1000);    // service time
            g_read_ops.Inc();
        } else {
            status = 882;
            LOG(WARNING, "ReadBlock #%ld fail offset: %ld len: %d\n",
                block_id, offset, read_len);
        }
        delete[] buf;
    }
    response->set_status(status);
    done->Run();
    if (block) {
        block->DecRef();
    }
}
void ChunkServerImpl::RemoveObsoleteBlocks(std::vector<int64_t> blocks) {
    for (size_t i = 0; i < blocks.size(); i++) {
        if (!_block_manager->RemoveBlock(blocks[i])) {
            LOG(INFO, "Remove block fail: #%ld ", blocks[i]);
        }
    }
}
void ChunkServerImpl::PullNewBlocks(std::vector<ReplicaInfo> new_replica_info) {
    PullBlockReportRequest report_request;
    report_request.set_sequence_id(0);
    report_request.set_chunkserver_id(_chunkserver_id);
    for (size_t i = 0; i < new_replica_info.size(); i++) {
        int64_t block_id = new_replica_info[i].block_id();
        ChunkServer_Stub* chunkserver = NULL;
        Block* block = _block_manager->FindBlock(block_id, true);
        if (!block) {
            LOG(WARNING, "Cant't create block: #%ld ", block_id);
            //ignore this block
            continue;
        }
        if (!_rpc_client->GetStub(new_replica_info[i].chunkserver_address(0),
                    &chunkserver)) {
            LOG(WARNING, "Can't connect to chunkserver: %s\n",
                    new_replica_info[i].chunkserver_address(0).c_str());
            //remove this block
            block->DecRef();
            _block_manager->RemoveBlock(block_id);
            report_request.add_blocks(block_id);
            continue;
        }
        int64_t seq = -1;
        int64_t offset = 0;
        bool success = true;
        while (1) {
            ReadBlockRequest request;
            ReadBlockResponse response;
            request.set_sequence_id(++seq);
            request.set_block_id(block_id);
            request.set_offset(offset);
            request.set_read_len(256 * 1024);
            bool ret = _rpc_client->SendRequest(chunkserver,
                                                &ChunkServer_Stub::ReadBlock,
                                                &request, &response, 15, 3);
            if (!ret || response.status() != 0) {
                success = false;
                break;
            }
            int32_t len = response.databuf().size();
            const char* buf = response.databuf().data();
            if (len) {
                if (!block->Write(seq, offset, buf, len)) {
                    success = false;
                    break;
                }
            } else {
                block->SetSliceNum(seq);
            }
            if (block->IsComplete() && _block_manager->CloseBlock(block)) {
                LOG(INFO, "Pull block: #%ld finish\n", block_id);
                break;
            }
            offset += len;
        }
        delete chunkserver;
        block->DecRef();
        if (!success) {
            _block_manager->RemoveBlock(block_id);
        }
        report_request.add_blocks(block_id);
    }

    PullBlockReportResponse report_response;
    NameServer_Stub* stub = NULL;
    {
        MutexLock lock(&_master_mutex);
        _rpc_client->GetStub(_master_nameserver_addr, &stub);
    }
    if (!_rpc_client->SendRequest(stub, &NameServer_Stub::PullBlockReport,
                &report_request, &report_response, 15, 3)) {
        LOG(WARNING, "Report pull finish fail, chunkserver id: %d\n", _chunkserver_id);
    } else {
        LOG(INFO, "Report pull finish dnne, chunkserver id: %d\n", _chunkserver_id);
    }
    delete stub;
}


void ChunkServerImpl::GetBlockInfo(::google::protobuf::RpcController* controller,
                                   const GetBlockInfoRequest* request,
                                   GetBlockInfoResponse* response,
                                   ::google::protobuf::Closure* done) {
    if (!response->has_sequence_id()) {
        response->set_sequence_id(request->sequence_id());
        response->add_timestamp(common::timer::get_micros());
        boost::function<void ()> task =
            boost::bind(&ChunkServerImpl::GetBlockInfo, this, controller, request, response, done);
        _read_thread_pool->AddTask(task);
        return;
    }
    int64_t block_id = request->block_id();
    int status = 0;

    int64_t find_start = common::timer::get_micros();
    Block* block = _block_manager->FindBlock(block_id, false);
    int64_t find_end = common::timer::get_micros();
    if (block == NULL) {
        status = 404;
        LOG(WARNING, "GetBlockInfo not found: #%ld ",block_id);
    } else {
        int64_t block_size = block->GetMeta().block_size;
        response->set_block_size(block_size);
        status = 0;
        LOG(INFO, "GetBlockInfo #%ld return: %d "
                  "use %ld %ld %ld %ld",
            block_id, block_size,
            (response->timestamp(0) - request->sequence_id()) / 1000, // rpc time
            (find_start - response->timestamp(0)) / 1000,   // dispatch time
            (find_end - find_start) / 1000, // find time
            (find_end - response->timestamp(0)) / 1000);    // service time
    }
    response->set_status(status);
    done->Run();

    if (block) block->DecRef();

}

} // namespace bfs

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
