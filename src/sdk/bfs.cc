// Copyright (c) 2014, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#include <gflags/gflags.h>

#include <fcntl.h>
#include <list>
#include <queue>
#include <sstream>
#include <vector>

#include "proto/nameserver.pb.h"
#include "proto/chunkserver.pb.h"
#include "rpc/rpc_client.h"
#include "file_location_cache.h"
#include "common/atomic.h"
#include "common/mutex.h"
#include "common/timer.h"
#include "common/sliding_window.h"
#include "common/logging.h"
#include "common/string_util.h"
#include "common/tprinter.h"

#include "bfs.h"

DECLARE_string(nameserver);
DECLARE_string(nameserver_port);
DECLARE_int32(sdk_thread_num);
DECLARE_int32(sdk_file_reada_len);
DECLARE_int32(file_location_cache_size);
DECLARE_string(sdk_write_mode);

namespace bfs {

ThreadPool g_thread_pool(FLAGS_sdk_thread_num);

struct LocatedBlocks {
    int64_t _file_length;
    std::vector<LocatedBlock> _blocks;
    void CopyFrom(const ::google::protobuf::RepeatedPtrField< ::bfs::LocatedBlock >& blocks) {
        for (int i = 0; i < blocks.size(); i++) {
            _blocks.push_back(blocks.Get(i));
        }
    }
};

class FSImpl;

class WriteBuffer {
public:
    WriteBuffer(int32_t seq, int32_t buf_size, int64_t block_id, int64_t offset)
        : _buf_size(buf_size), _data_size(0),
          _block_id(block_id), _offset(offset),
          _seq_id(seq), _is_last(false), _refs(0) {
        _buf = new char[buf_size];
    }
    ~WriteBuffer() {
        delete[] _buf;
        _buf = NULL;
    }
    int Available() {
        return _buf_size - _data_size;
    }
    int Append(const char* buf, int len) {
        assert(len + _data_size <= _buf_size);
        memcpy(_buf + _data_size, buf, len);
        _data_size += len;
        return _data_size;
    }
    const char* Data() {
        return _buf;
    }
    int Size() const {
        return _data_size;
    }
    int Sequence() const {
        return _seq_id;
    }
    void Clear() {
        _data_size = 0;
    }
    void SetLast() {
        _is_last = true;
    }
    bool IsLast() const { return _is_last; }
    int64_t offset() const { return _offset; }
    int64_t block_id() const { return _block_id; }
    void AddRefBy(int counter) {
        common::atomic_add(&_refs, counter);
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
    int32_t _buf_size;
    int32_t _data_size;
    char*   _buf;
    int64_t _block_id;
    int64_t _offset;
    int32_t _seq_id;
    bool    _is_last;
    volatile int _refs;
};

class BfsFileImpl : public File {
public:
    BfsFileImpl(FSImpl* fs, RpcClient* rpc_client, const std::string name, int32_t flags);
    ~BfsFileImpl ();
    int32_t Pread(char* buf, int32_t read_size, int64_t offset, bool reada = false);
    int64_t Seek(int64_t offset, int32_t whence);
    int32_t Read(char* buf, int32_t read_size);
    int32_t Write(const char* buf, int32_t write_size);
    /// Add buffer to  async write list
    void StartWrite(WriteBuffer *buffer);
    /// Send local buffer to chunkserver
    void BackgroundWrite();
    /// Callback for sliding window
    void OnWriteCommit(int32_t, int32_t);
    void WriteChunkCallback(const WriteBlockRequest* request,
                            WriteBlockResponse* response,
                            bool failed, int error,
                            int retry_times,
                            WriteBuffer* buffer,
                            std::string cs_addr);
    /// When rpc buffer full deley send write reqeust
    void DelayWriteChunk(WriteBuffer* buffer, const WriteBlockRequest* request,
                         int retry_times, std::string cs_addr);
    bool Flush();
    bool Sync();
    bool Close();

    struct WriteBufferCmp {
        bool operator()(const WriteBuffer* a, const WriteBuffer* b) {
            return a->Sequence() > b->Sequence();
        }
    };
    friend class FSImpl;
private:
    bool CheckWriteWindows();
private:
    FSImpl* _fs;                        ///< 文件系统
    RpcClient* _rpc_client;             ///< RpcClient
    std::string _name;                  ///< 文件路径
    int32_t _open_flags;                ///< 打开使用的flag

    /// for write
    LocatedBlock* _block_for_write;     ///< 正在写的block
    WriteBuffer* _write_buf;            ///< 本地写缓冲
    int32_t _last_seq;                  ///< last sequence number
    std::map<std::string, common::SlidingWindow<int>* > _write_windows;
    std::priority_queue<WriteBuffer*, std::vector<WriteBuffer*>, WriteBufferCmp>
        _write_queue;                   ///< Write buffer list
    volatile int _back_writing;         ///< Async write running backgroud

    /// for read
    LocatedBlocks _located_blocks;      ///< block meta for read
    ChunkServer_Stub* _chunkserver;     ///< located chunkserver
    std::map<string, ChunkServer_Stub*> _chunkservers; ///< located chunkservers
    int64_t _read_offset;               ///< 读取的偏移
    char* _reada_buffer;                ///< Read ahead buffer
    int32_t _reada_buf_len;             ///< Read ahead buffer length
    int64_t _reada_base;                ///< Read ahead base offset

    bool _closed;                       ///< 是否关闭
    Mutex   _mu;
    CondVar _sync_signal;               ///< _sync_var
    bool _bg_error;                     ///< background write error
    std::map<std::string, bool> cs_errors;        ///< background write error for each chunkserver
};

class FSImpl : public FS {
public:
    friend class BfsFileImpl;
    FSImpl() : _rpc_client(NULL), _nameserver(NULL) {
        _file_location_cache = new FileLocationCache(FLAGS_file_location_cache_size);
    }
    ~FSImpl() {
        delete _nameserver;
        delete _rpc_client;
        delete _file_location_cache;
    }
    bool ConnectNameServer(const char* nameserver) {
        if (nameserver != NULL) {
            _nameserver_address = nameserver;
        } else {
            _nameserver_address = FLAGS_nameserver + ":" + FLAGS_nameserver_port;
        }
        _rpc_client = new RpcClient();
        bool ret = _rpc_client->GetStub(_nameserver_address, &_nameserver);
        return ret;
    }
    bool CreateDirectory(const char* path) {
        CreateFileRequest request;
        CreateFileResponse response;
        request.set_file_name(path);
        request.set_mode(0755|(1<<9));
        request.set_sequence_id(0);
        bool ret = _rpc_client->SendRequest(_nameserver, &NameServer_Stub::CreateFile,
            &request, &response, 15, 3);
        if (!ret || response.status() != 0) {
            return false;
        } else {
            return true;
        }
    }
    bool ListDirectory(const char* path, BfsFileInfo** filelist, int *num) {
        common::timer::AutoTimer at(1000, "ListDirectory", path);
        *filelist = NULL;
        *num = 0;
        ListDirectoryRequest request;
        ListDirectoryResponse response;
        request.set_path(path);
        request.set_sequence_id(0);
        bool ret = _rpc_client->SendRequest(_nameserver, &NameServer_Stub::ListDirectory,
            &request, &response, 15, 3);
        if (!ret || response.status() != 0) {
            LOG(WARNING, "List fail: %s\n", path);
            return false;
        }
        if (response.files_size() != 0) {
            *num = response.files_size();
            *filelist = new BfsFileInfo[*num];
            for (int i = 0; i < *num; i++) {
                BfsFileInfo& binfo =(*filelist)[i];
                const FileInfo& info = response.files(i);
                binfo.ctime = info.ctime();
                binfo.mode = info.type();
                binfo.size = info.size();
                snprintf(binfo.name, sizeof(binfo.name), "%s", info.name().c_str());
            }
        }
        return true;
    }
    bool DeleteDirectory(const char* path, bool recursive) {
        DeleteDirectoryRequest request;
        DeleteDirectoryResponse response;
        request.set_sequence_id(0);
        request.set_path(path);
        request.set_recursive(recursive);
        bool ret = _rpc_client->SendRequest(_nameserver, &NameServer_Stub::DeleteDirectory,
                &request, &response, 15, 3);
        if (!ret) {
            LOG(WARNING, "DeleteDirectory fail: %s\n", path);
            return false;
        }
        if (response.status() == 404) {
            LOG(WARNING, "%s is not found.", path);
        }
        return response.status() == 0;
    }
    bool Access(const char* path, int32_t mode) {
        StatRequest request;
        StatResponse response;
        request.set_path(path);
        request.set_sequence_id(0);
        bool ret = _rpc_client->SendRequest(_nameserver, &NameServer_Stub::Stat,
            &request, &response, 15, 3);
        if (!ret) {
            LOG(WARNING, "Stat fail: %s\n", path);
            return false;
        }
        return (response.status() == 0);
    }
    bool Stat(const char* path, BfsFileInfo* fileinfo) {
        StatRequest request;
        StatResponse response;
        request.set_path(path);
        request.set_sequence_id(0);
        bool ret = _rpc_client->SendRequest(_nameserver, &NameServer_Stub::Stat,
            &request, &response, 15, 3);
        if (!ret) {
            fprintf(stderr, "Stat rpc fail: %s\n", path);
            return false;
        }
        if (response.status() == 0) {
            const FileInfo& info = response.file_info();
            fileinfo->ctime = info.ctime();
            fileinfo->mode = info.type();
            fileinfo->size = info.size();
            snprintf(fileinfo->name, sizeof(fileinfo->name), "%s", info.name().c_str());
            return true;
        }
        return false;
    }
    bool GetFileSize(const char* path, int64_t* file_size) {
        if (file_size == NULL) {
            return false;
        }
        std::vector<LocatedBlock> blocks;
        bool ret = _file_location_cache->GetFileLocation(path, &blocks);
        if (!ret) {
            ret = UpdateFileLocation(path, &blocks);
            if (!ret) {
                LOG(WARNING, "update file %s location fail", path);
                return false;
            }
        }

        *file_size = 0;
        for (size_t i = 0; i < blocks.size(); i++) {
            const LocatedBlock& block = blocks[i];
            if (block.block_size()) {
                *file_size += block.block_size();
                continue;
            }
            ChunkServer_Stub* chunkserver = NULL;
            bool available = false;
            for (int j = 0; j < block.chains_size(); j++) {
                std::string addr = block.chains(j).address();
                ret = _rpc_client->GetStub(addr, &chunkserver);
                if (!ret) {
                    LOG(INFO, "GetFileSize(%s) connect chunkserver fail %s",
                        path, addr.c_str());
                } else {
                    GetBlockInfoRequest gbi_request;
                    gbi_request.set_block_id(block.block_id());
                    gbi_request.set_sequence_id(common::timer::get_micros());
                    GetBlockInfoResponse gbi_response;
                    ret = _rpc_client->SendRequest(chunkserver, 
                        &ChunkServer_Stub::GetBlockInfo, &gbi_request, &gbi_response, 15, 3);
                    delete chunkserver;
                    if (!ret || gbi_response.status() != 0) {
                        LOG(INFO, "GetFileSize(%s) GetBlockInfo from chunkserver %s fail",
                            path, addr.c_str());
                        continue;
                    }
                    *file_size += gbi_response.block_size();
                    available = true;
                    break;
                }
            }
            if (!available) {
                LOG(WARNING, "GetFileSize(%s) fail no available chunkserver", path);
                return false;
            }
        }
        return true;
    }
    bool OpenFile(const char* path, int32_t flags, File** file) {
        common::timer::AutoTimer at(100, "OpenFile", path);
        bool ret = false;
        *file = NULL;
        if (flags & O_WRONLY) {
            CreateFileRequest request;
            CreateFileResponse response;
            request.set_file_name(path);
            request.set_sequence_id(0);
            request.set_flags(flags);
            request.set_mode(0644);
            ret = _rpc_client->SendRequest(_nameserver, &NameServer_Stub::CreateFile,
                &request, &response, 15, 3);
            if (!ret || response.status() != 0) {
                fprintf(stderr, "Open file for write fail: %s, status= %d\n",
                    path, response.status());
                ret = false;
            } else {
                //printf("Open file %s\n", path);
                *file = new BfsFileImpl(this, _rpc_client, path, flags);
            }
        } else if (flags == O_RDONLY) {
            std::vector<LocatedBlock> blocks;
            ret = _file_location_cache->GetFileLocation(path, &blocks);
            if (!ret) {
                ret = UpdateFileLocation(path, &blocks);
                if (!ret) {
                    LOG(WARNING, "Update file %s location fail", path);
                    return ret;
                }
            }
            BfsFileImpl* f = new BfsFileImpl(this, _rpc_client, path, flags);
            f->_located_blocks._blocks = blocks;
            *file = f;
        } else {
            LOG(WARNING, "Open flags only O_RDONLY or O_WRONLY\n");
            ret = false;
        }
        return ret;
    }
    bool CloseFile(File* file) {
        int64_t block_id = -1;
        int open_flags = -1;
        bool empty_file = false;
        BfsFileImpl* bfs_file = dynamic_cast<BfsFileImpl*>(file);
        if (bfs_file) {
            open_flags = bfs_file->_open_flags;
            if (!bfs_file->_block_for_write) {
                empty_file = true;
            }
            if (!empty_file && (open_flags & O_WRONLY)) {
                block_id = bfs_file->_block_for_write->block_id();
            }
        } else {
            LOG(WARNING, "Not a BfsFileImpl object?\n");
            return false;
        }
        if (!file->Close()) {
            LOG(WARNING, "Close file fail\n");
            return false;
        }
        if (!empty_file && (open_flags & O_WRONLY)) {
            FinishBlockRequest request;
            FinishBlockResponse response;
            request.set_sequence_id(0);
            request.set_block_id(block_id);
            request.set_block_version(bfs_file->_last_seq);
            bool ret = _rpc_client->SendRequest(_nameserver, &NameServer_Stub::FinishBlock,
                    &request, &response, 15, 3);

            return ret && response.status() == 0;
        } else {
            return true;
        }
    }
    bool DeleteFile(const char* path) {
        UnlinkRequest request;
        UnlinkResponse response;
        request.set_path(path);
        int64_t seq = common::timer::get_micros();
        request.set_sequence_id(seq);
        // printf("Delete file: %s\n", path);
        bool ret = _rpc_client->SendRequest(_nameserver, &NameServer_Stub::Unlink,
            &request, &response, 15, 1);
        if (!ret) {
            fprintf(stderr, "Unlink rpc fail: %s\n", path);
            return false;
        }
        if (response.status() != 0) {
            fprintf(stderr, "Unlink %s return: %d\n", path, response.status());
            return false;
        }
        return true;
    }
    bool Rename(const char* oldpath, const char* newpath) {
        RenameRequest request;
        RenameResponse response;
        request.set_oldpath(oldpath);
        request.set_newpath(newpath);
        request.set_sequence_id(0);
        bool ret = _rpc_client->SendRequest(_nameserver, &NameServer_Stub::Rename,
            &request, &response, 15, 3);
        if (!ret) {
            fprintf(stderr, "Rename rpc fail: %s to %s\n", oldpath, newpath);
            return false;
        }
        if (response.status() != 0) {
            fprintf(stderr, "Rename %s to %s return: %d\n",
                oldpath, newpath, response.status());
            return false;
        }
        return true;
    }
    bool ChangeReplicaNum(const char* file_name, int32_t replica_num) {
        ChangeReplicaNumRequest request;
        ChangeReplicaNumResponse response;
        request.set_file_name(file_name);
        request.set_replica_num(replica_num);
        request.set_sequence_id(0);
        bool ret = _rpc_client->SendRequest(_nameserver,
                &NameServer_Stub::ChangeReplicaNum,
                &request, &response, 15, 3);
        if (!ret) {
            fprintf(stderr, "Change %s replica num to %d rpc fail\n",
                    file_name, replica_num);
            return false;
        }
        if (response.status() != 0) {
            fprintf(stderr, "Change %s replica num to %d return: %d\n",
                    file_name, replica_num, response.status());
            return false;
        }
        return true;
    }
    bool SysStat(const std::string& stat_name, std::string* result) {
        SysStatRequest request;
        SysStatResponse response;
        bool ret = _rpc_client->SendRequest(_nameserver,
                &NameServer_Stub::SysStat,
                &request, &response, 15, 3);
        if (!ret) {
            LOG(WARNING, "SysStat fail %d", response.status());
            return false;
        }
        bool stat_all = (stat_name == "StatAll");
        common::TPrinter tp(7);
        tp.AddRow(7, "", "id", "address", "data_size", "blocks", "alive", "last_check");
        for (int i = 0; i < response.chunkservers_size(); i++) {
            const ChunkServerInfo& chunkserver = response.chunkservers(i);
            if (!stat_all && chunkserver.is_dead()) {
                continue;
            }
            std::vector<std::string> vs;
            vs.push_back(common::NumToString(i + 1));
            vs.push_back(common::NumToString(chunkserver.id()));
            vs.push_back(chunkserver.address());
            vs.push_back(common::HumanReadableString(chunkserver.data_size()) + "B");
            vs.push_back(common::NumToString(chunkserver.block_num()));
            vs.push_back(chunkserver.is_dead() ? "dead" : "alive");
            vs.push_back(common::NumToString(
                            common::timer::now_time() - chunkserver.last_heartbeat()));
            tp.AddRow(vs);
        }
        /*
        std::ostringstream oss;
        oss << "ChunkServer num: " << response.chunkservers_size() << std::endl
            << "Block num: " << response.block_num() << std::endl;
        result->assign(oss.str());*/
        result->append(tp.ToString());
        return true;
    }
private:
    bool UpdateFileLocation(const std::string& file_path, std::vector<LocatedBlock> *blocks) {
        FileLocationRequest request;
        FileLocationResponse response;
        request.set_file_name(file_path);
        request.set_sequence_id(0);
        bool ret = _rpc_client->SendRequest(_nameserver,
                &NameServer_Stub::GetFileLocation, &request, &response, 15, 3);
        if (!ret || response.status() != 0) {
            LOG(WARNING, "Update file location %s returns %d", file_path.c_str(), response.status());
            return false;
        }
        const ::google::protobuf::RepeatedPtrField< ::bfs::LocatedBlock >&
            response_blocks = response.blocks();
        for (int i = 0; i < response.blocks_size(); i++) {
            blocks->push_back(response_blocks.Get(i));
        }
        //fill cache
        _file_location_cache->FillCache(file_path, *blocks);
        return true;
    }
private:
    RpcClient* _rpc_client;
    NameServer_Stub* _nameserver;
    std::string _nameserver_address;
    FileLocationCache* _file_location_cache;
};

BfsFileImpl::BfsFileImpl(FSImpl* fs, RpcClient* rpc_client, 
                         const std::string name, int32_t flags)
  : _fs(fs), _rpc_client(rpc_client), _name(name),
    _open_flags(flags), _block_for_write(NULL),
    _write_buf(NULL), _last_seq(-1), _back_writing(0),
    _chunkserver(NULL), _read_offset(0), _reada_buffer(NULL),
    _reada_buf_len(0), _reada_base(0), _closed(false),
    _sync_signal(&_mu), _bg_error(false) {
}

BfsFileImpl::~BfsFileImpl () {
    if (!_closed) {
        _fs->CloseFile(this);
    }
    delete[] _reada_buffer;
    _reada_buffer = NULL;
    std::map<std::string, common::SlidingWindow<int>* >::iterator w_it;
    for (w_it = _write_windows.begin(); w_it != _write_windows.end(); ++w_it) {
        delete w_it->second;
        w_it->second = NULL;
    }
    std::map<std::string, ChunkServer_Stub*>::iterator it;
    for (it = _chunkservers.begin(); it != _chunkservers.end(); ++it) {
        delete it->second;
        it->second = NULL;
    }
}

int32_t BfsFileImpl::Pread(char* buf, int32_t read_len, int64_t offset, bool reada) {
    {
        MutexLock lock(&_mu, "Pread read buffer", 1000);
        if (_reada_buffer && _reada_base <= offset &&
                _reada_base + _reada_buf_len >= offset + read_len) {
            memcpy(buf, _reada_buffer + (offset - _reada_base), read_len);
            //LOG(INFO, "Read %s %ld from cache %ld", _name.c_str(), offset, read_len);
            return read_len;
        }
    }
    LocatedBlock lcblock;
    ChunkServer_Stub* chunk_server = NULL;
    std::string cs_addr;

    int server_index = 0;
    {
        MutexLock lock(&_mu, "Pread GetStub", 1000);
        if (_located_blocks._blocks.empty()) {
            return 0;
        } else if (_located_blocks._blocks[0].chains_size() == 0) {
            LOG(WARNING, "No located servers or _located_blocks[%lu]",
                _located_blocks._blocks.size());
            return -3;
        }
        lcblock.CopyFrom(_located_blocks._blocks[0]);
        server_index = rand() % lcblock.chains_size();
        cs_addr = lcblock.chains(server_index).address();
        if (_chunkserver == NULL) {
            _fs->_rpc_client->GetStub(cs_addr, &_chunkserver);
        }
        chunk_server = _chunkserver;
    }
    int64_t block_id = lcblock.block_id();

    ReadBlockRequest request;
    ReadBlockResponse response;
    request.set_sequence_id(common::timer::get_micros());
    request.set_block_id(block_id);
    request.set_offset(offset);
    int32_t rlen = read_len;
    if (reada && read_len < FLAGS_sdk_file_reada_len) {
        rlen = FLAGS_sdk_file_reada_len;
    }
    request.set_read_len(rlen);
    bool ret = false;
    bool update_location = false;

    for (int retry_times = 0; retry_times < lcblock.chains_size() * 2; retry_times++) {
        if (retry_times == lcblock.chains_size() && !update_location) {
            // update block location
            update_location = true;
            _fs->UpdateFileLocation(_name, &_located_blocks._blocks);
            retry_times = lcblock.chains_size() - 1;
            server_index = rand() % lcblock.chains_size();
            cs_addr = lcblock.chains(server_index).address();
            {
                MutexLock lock(&_mu, "Pread change _chunkserver", 1000);
                delete _chunkserver;
                _fs->_rpc_client->GetStub(cs_addr, &_chunkserver);
            }
            chunk_server = _chunkserver;
            continue;
        }
        LOG(DEBUG, "Start Pread: %s", cs_addr.c_str());
        ret = _fs->_rpc_client->SendRequest(chunk_server, &ChunkServer_Stub::ReadBlock,
                    &request, &response, 15, 3);

        if (!ret || response.status() != 0) {
            ///TODO: Add to _bad_chunkservers
            cs_addr = lcblock.chains((++server_index) % lcblock.chains_size()).address();
            LOG(INFO, "Pread retry another chunkserver: %s", cs_addr.c_str());
            delete chunk_server;
            _fs->_rpc_client->GetStub(cs_addr, &chunk_server);
            {
                MutexLock lock(&_mu, "Pread change _chunkserver", 1000);
                _chunkserver = chunk_server;
            }
        } else {
            break;
        }
    }

    if (!ret || response.status() != 0) {
        printf("Read block %ld fail, status= %d\n", block_id, response.status());
        return -4;
    }

    //printf("Pread[%s:%ld:%ld] return %lu bytes\n",
    //       _name.c_str(), offset, read_len, response.databuf().size());
    int32_t ret_len = response.databuf().size();
    if (read_len < ret_len) {
        MutexLock lock(&_mu, "Pread fill buffer", 1000);
        int32_t cache_len = ret_len - read_len;
        if (cache_len > _reada_buf_len) {
            delete[] _reada_buffer;
            _reada_buffer = new char[cache_len];
        }
        _reada_buf_len = cache_len;
        memcpy(_reada_buffer, response.databuf().data() + read_len, cache_len);
        _reada_base = offset + read_len;
        ret_len = read_len;
    }
    assert(read_len >= ret_len);
    memcpy(buf, response.databuf().data(), ret_len);
    return ret_len;
}

int64_t BfsFileImpl::Seek(int64_t offset, int32_t whence) {
    //printf("Seek[%s:%d:%ld]\n", _name.c_str(), whence, offset);
    if (_open_flags != O_RDONLY) {
        return -2;
    }
    if (whence == SEEK_SET) {
        _read_offset = offset;
    } else if (whence == SEEK_CUR) {
        _read_offset += offset;
    } else {
        return -1;
    }
    return _read_offset;
}

int32_t BfsFileImpl::Read(char* buf, int32_t read_len) {
    //LOG(DEBUG, "[%p] Read[%s:%ld] offset= %ld\n",
    //    this, _name.c_str(), read_len, _read_offset);
    if (_open_flags != O_RDONLY) {
        return -2;
    }
    int32_t ret = Pread(buf, read_len, _read_offset, true);
    //LOG(INFO, "Read[%s:%ld,%ld] return %d", _name.c_str(), _read_offset, read_len, ret);
    if (ret >= 0) {
        _read_offset += ret;
    }
    return ret;
}

int32_t BfsFileImpl::Write(const char* buf, int32_t len) {
    common::timer::AutoTimer at(100, "Write", _name.c_str());

    {
        MutexLock lock(&_mu, "Write", 1000);
        if (!(_open_flags & O_WRONLY)) {
            return -2;
        } else if (_bg_error) {
            return -3;
        } else if (_closed) {
            return -4;
        }
        common::atomic_inc(&_back_writing);
    }
    if (_open_flags & O_WRONLY) {
        MutexLock lock(&_mu, "Write AddBlock", 1000);
        // Add block
        if (_chunkservers.empty()) {
            AddBlockRequest request;
            AddBlockResponse response;
            request.set_sequence_id(0);
            request.set_file_name(_name);
            bool ret = _rpc_client->SendRequest(_fs->_nameserver, &NameServer_Stub::AddBlock,
                &request, &response, 15, 3);
            if (!ret || !response.has_block()) {
                LOG(WARNING, "AddBlock fail for %s\n", _name.c_str());
                common::atomic_dec(&_back_writing); 
                return -1;
            }
            _block_for_write = new LocatedBlock(response.block());
            int cs_size = FLAGS_sdk_write_mode == "chains" ? 1 :
                                                    _block_for_write->chains_size();
            for (int i = 0; i < cs_size; i++) {
                const std::string& addr = _block_for_write->chains(i).address();
                _rpc_client->GetStub(addr, &_chunkservers[addr]);
                _write_windows[addr] = new common::SlidingWindow<int>(100,
                                       boost::bind(&BfsFileImpl::OnWriteCommit, this, _1, _2));
                cs_errors[addr] = false;
            }
        }
    }

    int32_t w = 0;
    while (w < len) {
        MutexLock lock(&_mu, "WriteInternal", 1000);
        if (_write_buf == NULL) {
            _write_buf = new WriteBuffer(++_last_seq, 256*1024,
                                         _block_for_write->block_id(),
                                         _block_for_write->block_size());
        }
        if ( (len - w) < _write_buf->Available()) {
            _write_buf->Append(buf+w, len-w);
            w = len;
            break;
        } else {
            int n = _write_buf->Available();
            _write_buf->Append(buf+w, n);
            w += n;
        }
        if (_write_buf->Available() == 0) {
            StartWrite(_write_buf);
        }
    }
    // printf("Write return %d, buf_size=%d\n", w, file->_write_buf->Size());
    common::atomic_dec(&_back_writing); 
    return w;
}

void BfsFileImpl::StartWrite(WriteBuffer *buffer) {
    common::timer::AutoTimer at(5, "StartWrite", _name.c_str());
    _mu.AssertHeld();
    _write_queue.push(_write_buf);
    _block_for_write->set_block_size(_block_for_write->block_size() + _write_buf->Size());
    _write_buf = NULL;
    boost::function<void ()> task = 
        boost::bind(&BfsFileImpl::BackgroundWrite, this);
    common::atomic_inc(&_back_writing);
    _mu.Unlock();
    g_thread_pool.AddTask(task);
    _mu.Lock("StartWrite relock", 1000);
}

bool BfsFileImpl::CheckWriteWindows() {
    _mu.AssertHeld();
    if (FLAGS_sdk_write_mode == "chains") {
        return _write_windows.begin()->second->UpBound() > _write_queue.top()->Sequence();
    }
    std::map<std::string, common::SlidingWindow<int>* >::iterator it;
    int count = 0;
    for (it = _write_windows.begin(); it != _write_windows.end(); ++it) {
        if (it->second->UpBound() > _write_queue.top()->Sequence()) {
            count++;
        }
    }
    return count >= (int)_write_windows.size() - 1;
}

/// Send local buffer to chunkserver
void BfsFileImpl::BackgroundWrite() {
    MutexLock lock(&_mu, "BackgroundWrite", 1000);
    while(!_write_queue.empty() && CheckWriteWindows()) {
        WriteBuffer* buffer = _write_queue.top();
        _write_queue.pop();
        _mu.Unlock();

        buffer->AddRefBy(_chunkservers.size());
        for (size_t i = 0; i < _chunkservers.size(); i++) {
            std::string cs_addr = _block_for_write->chains(i).address();
            bool delay = false;
            if (!(_write_windows[cs_addr]->UpBound() > _write_queue.top()->Sequence())) {
                delay = true;
            }
            {
                // skip bad chunkserver
                ///TODO improve here?
                MutexLock lock(&_mu);
                if (cs_errors[cs_addr]) {
                    buffer->DecRef();
                    continue;
                }
            }
            WriteBlockRequest* request = new WriteBlockRequest;
            WriteBlockResponse* response = new WriteBlockResponse;
            int64_t offset = buffer->offset();
            int64_t seq = common::timer::get_micros();
            request->set_sequence_id(seq);
            request->set_block_id(buffer->block_id());
            request->set_databuf(buffer->Data(), buffer->Size());
            request->set_offset(offset);
            request->set_is_last(buffer->IsLast());
            request->set_packet_seq(buffer->Sequence());
            //request->add_desc("start");
            //request->add_timestamp(common::timer::get_micros());
            if (FLAGS_sdk_write_mode == "chains") {
                for (int i = 1; i < _block_for_write->chains_size(); i++) {
                    std::string addr = _block_for_write->chains(i).address();
                    request->add_chunkservers(addr);
                }
            }
            const int max_retry_times = 5;
            ChunkServer_Stub* stub = _chunkservers[cs_addr];
            boost::function<void (const WriteBlockRequest*, WriteBlockResponse*, bool, int)> callback
                = boost::bind(&BfsFileImpl::WriteChunkCallback, this, _1, _2, _3, _4,
                        max_retry_times, buffer, cs_addr);

            LOG(DEBUG, "BackgroundWrite start [bid:%ld, seq:%d, offset:%ld, len:%d]\n",
                    buffer->block_id(), buffer->Sequence(), buffer->offset(), buffer->Size());
            common::atomic_inc(&_back_writing);
            if (delay) {
                g_thread_pool.DelayTask(5,
                        boost::bind(&BfsFileImpl::DelayWriteChunk, this, buffer,
                            request, max_retry_times, cs_addr));
            } else {
                _rpc_client->AsyncRequest(stub, &ChunkServer_Stub::WriteBlock,
                        request, response, callback, 60, 1);
            }
        }
        _mu.Lock("BackgroundWriteRelock", 1000);
    }
    common::atomic_dec(&_back_writing);    // for AddTask
}

void BfsFileImpl::DelayWriteChunk(WriteBuffer* buffer,
                                  const WriteBlockRequest* request,
                                  int retry_times, std::string cs_addr) {
    WriteBlockResponse* response = new WriteBlockResponse;
    boost::function<void (const WriteBlockRequest*, WriteBlockResponse*, bool, int)> callback
        = boost::bind(&BfsFileImpl::WriteChunkCallback, this, _1, _2, _3, _4,
                      retry_times, buffer, cs_addr);
    common::atomic_inc(&_back_writing);
    ChunkServer_Stub* stub = _chunkservers[cs_addr];
    _rpc_client->AsyncRequest(stub, &ChunkServer_Stub::WriteBlock,
        request, response, callback, 60, 1);

    common::atomic_dec(&_back_writing);    // for DelayTask
}

void BfsFileImpl::WriteChunkCallback(const WriteBlockRequest* request,
                                     WriteBlockResponse* response,
                                     bool failed, int error,
                                     int retry_times,
                                     WriteBuffer* buffer,
                                     std::string cs_addr) {
    if (failed || response->status() != 0) {
        if (sofa::pbrpc::RPC_ERROR_SEND_BUFFER_FULL != error
                && response->status() != 500) {
            if (retry_times < 5) {
                LOG(INFO, "BackgroundWrite failed %s"
                    " #%ld seq:%d, offset:%ld, len:%d"
                    " status: %d, retry_times: %d",
                    _name.c_str(),
                    buffer->block_id(), buffer->Sequence(),
                    buffer->offset(), buffer->Size(),
                    response->status(), retry_times);
            }
            if (--retry_times == 0) {
                LOG(WARNING, "BackgroundWrite error %s"
                    "#%ld seq:%d, offset:%ld, len:%d]"
                    " status: %d, retry_times: %d",
                    _name.c_str(),
                    buffer->block_id(), buffer->Sequence(),
                    buffer->offset(), buffer->Size(),
                    response->status(), retry_times);
                ///TODO: SetFaild & handle it
                if (FLAGS_sdk_write_mode == "chains") {
                    _bg_error = true;
                } else {
                    MutexLock lock(&_mu);
                    cs_errors[cs_addr] = true;
                    std::map<std::string, bool>::iterator it = cs_errors.begin();
                    int count = 0;
                    for (; it != cs_errors.end(); ++it) {
                        if (it->second == true) {
                            count++;
                        }
                    }
                    if (count > 1) {
                        _bg_error = true;
                    }
                }
                buffer->DecRef();
                delete request;
            }
        }
        if (!_bg_error) {
            common::atomic_inc(&_back_writing);
            g_thread_pool.DelayTask(5,
                boost::bind(&BfsFileImpl::DelayWriteChunk, this, buffer,
                            request, retry_times, cs_addr));
        }
    } else {
        LOG(DEBUG, "BackgroundWrite done bid:%ld, seq:%d, offset:%ld, len:%d, _back_writing:%d",
            buffer->block_id(), buffer->Sequence(), buffer->offset(), 
            buffer->Size(), _back_writing);
        int r = _write_windows[cs_addr]->Add(buffer->Sequence(), 0);
        assert(r == 0);
        buffer->DecRef();
        delete request;
    }
    delete response;

    {
        MutexLock lock(&_mu, "WriteChunkCallback", 1000);
        if (_write_queue.empty() || _bg_error) {
            common::atomic_dec(&_back_writing);    // for AsyncRequest
            if (_back_writing == 0) {
                _sync_signal.Broadcast();
            }
            return;
        }
    }

    boost::function<void ()> task =
        boost::bind(&BfsFileImpl::BackgroundWrite, this);
    g_thread_pool.AddTask(task);
}

void BfsFileImpl::OnWriteCommit(int32_t, int) {
}

bool BfsFileImpl::Flush() {
    // Not impliment
    return true;
}
bool BfsFileImpl::Sync() {
    common::timer::AutoTimer at(50, "Sync", _name.c_str());
    if (_open_flags != O_WRONLY) {
        return false;
    }
    MutexLock lock(&_mu, "Sync", 1000);
    if (_write_buf && _write_buf->Size()) {
        StartWrite(_write_buf);
    }
    int wait_time = 0;
    while (_back_writing) {
        bool finish = _sync_signal.TimeWait(1000, "Sync wait");
        if (++wait_time >= 30 && (wait_time % 10 == 0)) {
            LOG(WARNING, "Sync timeout %d s, %s _back_writing= %d, finish= %d",
                wait_time, _name.c_str(), _back_writing, finish);
        }
    }
    // fprintf(stderr, "Sync %s fail\n", _name.c_str());
    return !_bg_error;
}

bool BfsFileImpl::Close() {
    common::timer::AutoTimer at(500, "Close", _name.c_str());
    MutexLock lock(&_mu, "Close", 1000);
    if (_block_for_write && (_open_flags & O_WRONLY)) {
        if (!_write_buf) {
            _write_buf = new WriteBuffer(++_last_seq, 32, _block_for_write->block_id(),
                                         _block_for_write->block_size());
        }
        _write_buf->SetLast();
        StartWrite(_write_buf);

        //common::timer::AutoTimer at(1, "LastWrite", _name.c_str());
        int wait_time = 0;
        while (_back_writing) {
            bool finish = _sync_signal.TimeWait(1000, (_name + " Close wait").c_str());
            if (++wait_time >= 30 && (wait_time % 10 == 0)) {
                LOG(WARNING, "Close timeout %d s, %s _back_writing= %d, finish= %d",
                wait_time, _name.c_str(), _back_writing, finish);
            }
        }
        delete _block_for_write;
        _block_for_write = NULL;
    }
    delete _chunkserver;
    _chunkserver = NULL;
    LOG(DEBUG, "File %s closed", _name.c_str());
    _closed = true;
    return !_bg_error;
}

bool FS::OpenFileSystem(const char* nameserver, FS** fs) {
    FSImpl* impl = new FSImpl;
    if (!impl->ConnectNameServer(nameserver)) {
        *fs = NULL;
        return false;
    }
    *fs = impl;
    g_thread_pool.Start();
    return true;
}

}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
