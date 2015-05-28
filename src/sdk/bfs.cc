// Copyright (c) 2014, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#include <gflags/gflags.h>

#include <fcntl.h>
#include <list>
#include <queue>

#include "proto/nameserver.pb.h"
#include "proto/chunkserver.pb.h"
#include "rpc/rpc_client.h"
#include "common/atomic.h"
#include "common/mutex.h"
#include "common/timer.h"
#include "common/sliding_window.h"
#include "common/logging.h"
#include "bfs.h"

DECLARE_string(nameserver);
DECLARE_string(nameserver_port);

namespace bfs {

ThreadPool g_thread_pool(4);

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
          _seq_id(seq), _is_last(false) {
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
private:
    int32_t _buf_size;
    int32_t _data_size;
    char*   _buf;
    int64_t _block_id;
    int64_t _offset;
    int32_t _seq_id;
    bool    _is_last;
};

class BfsFileImpl : public File {
public:
    BfsFileImpl(FSImpl* fs, RpcClient* rpc_client, const std::string name, int32_t flags);
    ~BfsFileImpl ();
    int64_t Pread(char* buf, int64_t read_size, int64_t offset);
    int64_t Seek(int64_t offset, int32_t whence);
    int64_t Read(char* buf, int64_t read_size);
    int64_t Write(const char* buf, int64_t write_size);
    /// Add buffer to  async write list
    void StartWrite(WriteBuffer *buffer);
    /// Send local buffer to chunkserver
    void BackgroundWrite(ChunkServer_Stub* stub);
    /// Callback for sliding window
    void OnWriteCommit(int32_t, int32_t);
    void WriteChunkCallback(const WriteBlockRequest* request,
                            WriteBlockResponse* response,
                            bool failed, int error,
                            int retry_times,
                            WriteBuffer* buffer);
    /// When rpc buffer full deley send write reqeust
    void DelayWriteChunk(WriteBuffer* buffer, const WriteBlockRequest* request,
                         int retry_times);
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
    FSImpl* _fs;                        ///< 文件系统
    RpcClient* _rpc_client;             ///< RpcClient
    std::string _name;                  ///< 文件路径
    int32_t _open_flags;                ///< 打开使用的flag

    /// for write
    ChunkServer_Stub* _chains_head;     ///< 对应的第一个chunkserver
    LocatedBlock* _block_for_write;     ///< 正在写的block
    WriteBuffer* _write_buf;            ///< 本地写缓冲
    int32_t _last_seq;                  ///< last sequence number
    common::SlidingWindow<int>* _write_window; ///< Send sliding window
    std::priority_queue<WriteBuffer*, std::vector<WriteBuffer*>, WriteBufferCmp>
        _write_queue;                   ///< Write buffer list
    volatile int _back_writing;         ///< Async write running backgroud

    /// for read
    LocatedBlocks _located_blocks;      ///< block meta for read
    ChunkServer_Stub* _chunkserver;     ///< located chunkserver
    int64_t _read_offset;               ///< 读取的偏移

    bool _closed;                       ///< 是否关闭
    Mutex   _mu;
    CondVar _sync_signal;               ///< _sync_var
};

class FSImpl : public FS {
public:
    friend class BfsFileImpl;
    FSImpl() : _rpc_client(NULL), _nameserver(NULL) {
    }
    ~FSImpl() {
        delete _nameserver;
        delete _rpc_client;
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
            &request, &response, 5, 3);
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
            &request, &response, 5, 3);
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
                &request, &response, 5, 3);
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
            &request, &response, 5, 3);
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
            &request, &response, 5, 3);
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
                &request, &response, 5, 3);
            if (!ret || response.status() != 0) {
                fprintf(stderr, "Open file for write fail: %s, status= %d\n",
                    path, response.status());
                ret = false;
            } else {
                //printf("Open file %s\n", path);
                *file = new BfsFileImpl(this, _rpc_client, path, flags);
            }
        } else if (flags == O_RDONLY) {
            FileLocationRequest request;
            FileLocationResponse response;
            request.set_file_name(path);
            request.set_sequence_id(0);
            ret = _rpc_client->SendRequest(_nameserver, &NameServer_Stub::GetFileLocation,
                &request, &response, 5, 3);
            if (ret && response.status() == 0) {
                BfsFileImpl* f = new BfsFileImpl(this, _rpc_client, path, flags);
                f->_located_blocks.CopyFrom(response.blocks());
                *file = f;
                //printf("OpenFile success: %s\n", path);
            } else {
                //printf("GetFileLocation return %d\n", response.blocks_size());
                fprintf(stderr, "OpenFile return %d\n", response.status());
                ret = false;
            }
        } else if (flags == O_APPEND) {
            *file = new BfsFileImpl(this, _rpc_client, path, flags);
            ret = true;
        } else {
            LOG(WARNING, "Open flags only O_RDONLY or O_WRONLY or O_APPEND\n");
            ret = false;
        }
        return ret;
    }
    bool CloseFile(File* file) {
        int64_t block_id = -1;
        int open_flags = -1;
        BfsFileImpl* bfs_file = dynamic_cast<BfsFileImpl*>(file);
        if (bfs_file) {
            open_flags = bfs_file->_open_flags;
            if (bfs_file->_block_for_write &&
                ((open_flags & O_WRONLY) || open_flags == O_APPEND)) {
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
        if (bfs_file->_block_for_write &&
            ((open_flags & O_WRONLY) || open_flags == O_APPEND)) {
            FinishBlockRequest request;
            FinishBlockResponse response;
            request.set_sequence_id(0);
            request.set_block_id(block_id);
            bool ret = _rpc_client->SendRequest(_nameserver, &NameServer_Stub::FinishBlock,
                    &request, &response, 5, 3);

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
            &request, &response, 5, 1);
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
            &request, &response, 5, 3);
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
                &request, &response, 5, 3);
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
private:
    RpcClient* _rpc_client;
    NameServer_Stub* _nameserver;
    std::string _nameserver_address;
};

BfsFileImpl::BfsFileImpl(FSImpl* fs, RpcClient* rpc_client, 
                         const std::string name, int32_t flags)
  : _fs(fs), _rpc_client(rpc_client), _name(name),
    _open_flags(flags), _chains_head(NULL), _block_for_write(NULL),
    _write_buf(NULL), _last_seq(-1), _back_writing(0),
    _chunkserver(NULL), _read_offset(0), _closed(false),
    _sync_signal(&_mu) {
    _write_window = new common::SlidingWindow<int>(100,
                        boost::bind(&BfsFileImpl::OnWriteCommit, this, _1, _2));
}

BfsFileImpl::~BfsFileImpl () {
    if (!_closed) {
        _fs->CloseFile(this);
    }
    delete _write_window;
}

int64_t BfsFileImpl::Pread(char* buf, int64_t read_len, int64_t offset) {
    MutexLock lock(&_mu, "Pread");
     
    if (_located_blocks._blocks.empty()) {
        return 0;
    } else if (_located_blocks._blocks[0].chains_size() == 0) {
        printf("No located servers or _located_blocks[%lu]\n", _located_blocks._blocks.size());
        return -3;
    }
    LocatedBlock& lcblock = _located_blocks._blocks[0];
    int64_t block_id = lcblock.block_id();
    ReadBlockRequest request;
    ReadBlockResponse response;
    request.set_sequence_id(0);
    request.set_block_id(block_id);
    request.set_offset(offset);
    request.set_read_len(read_len);
    bool ret = false;
    int retry_times = 0;
    int next_server = 0;
    if (_chunkserver == NULL) {
        srand(time(NULL));
        int first_server = rand() % lcblock.chains_size();
        _fs->_rpc_client->GetStub(lcblock.chains(first_server).address(), &_chunkserver);
    }

    while (!ret || response.status() != 0) {
        if (retry_times++ > lcblock.chains_size()) {
            break;
        }
        if (retry_times == 1) {
            ret = _fs->_rpc_client->SendRequest(_chunkserver, &ChunkServer_Stub::ReadBlock,
                    &request, &response, 5, 3);
        } else {
            _fs->_rpc_client->GetStub(lcblock.chains(next_server++).address(), &_chunkserver);
            ret = _fs->_rpc_client->SendRequest(_chunkserver, &ChunkServer_Stub::ReadBlock,
                    &request, &response, 5, 3);
        }
    }

    if (!ret || response.status() != 0) {
        printf("Read block %ld fail, status= %d\n", block_id, response.status());
        return -4;
    }

    //printf("Pread[%s:%ld:%ld] return %lu bytes\n",
    //       _name.c_str(), offset, read_len, response.databuf().size());
    int64_t ret_len = response.databuf().size();
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

int64_t BfsFileImpl::Read(char* buf, int64_t read_len) {
    //LOG(DEBUG, "[%p] Read[%s:%ld] offset= %ld\n",
    //    this, _name.c_str(), read_len, _read_offset);
    if (_open_flags != O_RDONLY) {
        return -2;
    }
    int ret = Pread(buf, read_len, _read_offset);
    if (ret >= 0) {
        _read_offset += ret;
    }
    //LOG(DEBUG, "[%p] Read[%s:%ld] return %d offset=%ld\n",
    //    this, _name.c_str(), read_len, ret, _read_offset);
    return ret;
}

int64_t BfsFileImpl::Write(const char* buf, int64_t len) {
    common::timer::AutoTimer at(100, "Write", _name.c_str());
    if (!(_open_flags & O_WRONLY) && !(_open_flags & O_APPEND)) {
        return -2;
    }
    if (_open_flags & O_WRONLY) {
        MutexLock lock(&_mu, "Write");
        // Add block
        if (_chains_head == NULL) {
            AddBlockRequest request;
            AddBlockResponse response;
            request.set_sequence_id(0);
            request.set_file_name(_name);
            bool ret = _rpc_client->SendRequest(_fs->_nameserver, &NameServer_Stub::AddBlock,
                &request, &response, 5, 3);
            if (!ret || !response.has_block()) {
                LOG(WARNING, "AddBlock fail for %s\n", _name.c_str());
                return -1;
            }
            _block_for_write = new LocatedBlock(response.block());
            const std::string& addr = _block_for_write->chains(0).address();
            //printf("response addr %s\n", response.block().chains(0).address().c_str());
            //printf("_block_for_write addr %s\n", 
            //        file->_block_for_write->chains(0).address().c_str());
            _rpc_client->GetStub(addr, &_chains_head);
        }
    } else if (_open_flags == O_APPEND) {
        if (_chains_head == NULL) {
            MutexLock lock(&_mu, "Append");
            FileLocationRequest request;
            FileLocationResponse response;
            request.set_file_name(_name);
            request.set_sequence_id(0);

            bool ret = _rpc_client->SendRequest(_fs->_nameserver,
                    &NameServer_Stub::GetFileLocation,
                    &request, &response, 5, 3);
            if (ret && response.status() == 0) {
                _block_for_write = new LocatedBlock(response.blocks(0));
                const std::string& addr = _block_for_write->chains(0).address();
                _rpc_client->GetStub(addr, &_chains_head);
            } else {
                fprintf(stderr, "Locate file %s error: %d\n", _name.c_str(), response.status());
                return -1;
            }
        }
    }

    int w = 0;
    while (w < len) {
        MutexLock lock(&_mu, "WriteInternal");
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
    return w;
}

void BfsFileImpl::StartWrite(WriteBuffer *buffer) {
    common::timer::AutoTimer at(200, "StartWrite", _name.c_str());
    _mu.AssertHeld();
    _write_queue.push(_write_buf);
    _block_for_write->set_block_size(_block_for_write->block_size() + _write_buf->Size());
    _write_buf = NULL;
    boost::function<void ()> task = 
        boost::bind(&BfsFileImpl::BackgroundWrite, this, _chains_head);
    common::atomic_inc(&_back_writing);
    g_thread_pool.AddTask(task);
}

/// Send local buffer to chunkserver
void BfsFileImpl::BackgroundWrite(ChunkServer_Stub* stub) {
    MutexLock lock(&_mu, "BackgroundWrite");
    while(!_write_queue.empty() && 
          _write_window->UpBound() > _write_queue.top()->Sequence()) {
        WriteBuffer* buffer = _write_queue.top();
        _write_queue.pop();
        _mu.Unlock();
        
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
        if (_open_flags == O_WRONLY) {
            request->set_is_append(false);
        } else {
            request->set_is_append(true);
        }
        //request->add_desc("start");
        //request->add_timestamp(common::timer::get_micros());
        
        for (int i = 1; i < _block_for_write->chains_size(); i++) {
            request->add_chunkservers(_block_for_write->chains(i).address());
        }

        const int max_retry_times = 5;
        boost::function<void (const WriteBlockRequest*, WriteBlockResponse*, bool, int)> callback
            = boost::bind(&BfsFileImpl::WriteChunkCallback, this, _1, _2, _3, _4, 
                          max_retry_times, buffer);

        LOG(DEBUG, "BackgroundWrite start [bid:%ld, seq:%d, offset:%ld, len:%d]\n",
                buffer->block_id(), buffer->Sequence(), buffer->offset(), buffer->Size());
        common::atomic_inc(&_back_writing);
        _rpc_client->AsyncRequest(stub, &ChunkServer_Stub::WriteBlock,
            request, response, callback, 60, 1);
        _mu.Lock("BackgroundWriteRelock");
    }
    common::atomic_dec(&_back_writing);    // for AddTask
}

void BfsFileImpl::DelayWriteChunk(WriteBuffer* buffer,
                                  const WriteBlockRequest* request,
                                  int retry_times) {
    WriteBlockResponse* response = new WriteBlockResponse;
    boost::function<void (const WriteBlockRequest*, WriteBlockResponse*, bool, int)> callback
        = boost::bind(&BfsFileImpl::WriteChunkCallback, this, _1, _2, _3, _4,
                      retry_times, buffer);
    common::atomic_inc(&_back_writing);
    _rpc_client->AsyncRequest(_chains_head, &ChunkServer_Stub::WriteBlock,
        request, response, callback, 60, 1);

    common::atomic_dec(&_back_writing);    // for DelayTask
}

void BfsFileImpl::WriteChunkCallback(const WriteBlockRequest* request,
                                     WriteBlockResponse* response,
                                     bool failed, int error,
                                     int retry_times,
                                     WriteBuffer* buffer) {
    if (failed || response->status() != 0) {
        if (sofa::pbrpc::RPC_ERROR_SEND_BUFFER_FULL != error) {
            LOG(WARNING, "BackgroundWrite failed [bid:%ld, seq:%d, offset:%ld, len:%d]"
                " status: %d, retry_times: %d",
                buffer->block_id(), buffer->Sequence(),
                buffer->offset(), buffer->Size(),
                response->status(), retry_times);
            if (--retry_times == 0) {
                ///TODO: SetFaild & handle it
                /// If there is a chunkserver failed, abandon this block, and add a new block ..............
                assert(0);
            }
        }
        common::atomic_inc(&_back_writing);
        g_thread_pool.DelayTask(
            5, boost::bind(&BfsFileImpl::DelayWriteChunk, this, buffer, request, retry_times));
    } else {
        LOG(DEBUG, "BackgroundWrite done [bid:%ld, seq:%d, offset:%ld, len:%d]\n",
            buffer->block_id(), buffer->Sequence(), buffer->offset(), buffer->Size());
        int r = _write_window->Add(buffer->Sequence(), 0);
        assert(r == 0);
        delete buffer;
        delete request;
    }
    delete response;

    {
        MutexLock lock(&_mu, "WriteChunkCallback");
        if (_write_queue.empty()) {
            common::atomic_dec(&_back_writing);    // for AsyncRequest
            if (_back_writing == 0) {
                _sync_signal.Broadcast();
            }
            return;
        }
    }

    boost::function<void ()> task =
        boost::bind(&BfsFileImpl::BackgroundWrite, this, _chains_head);
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
    MutexLock lock(&_mu, "Sync");
    if (_write_buf && _write_buf->Size()) {
        StartWrite(_write_buf);
    }
    while (_back_writing) {
        _sync_signal.Wait((_name + " Sync wait").c_str());
    }
    // fprintf(stderr, "Sync %s fail\n", _name.c_str());
    return true;
}

bool BfsFileImpl::Close() {
    common::timer::AutoTimer at(500, "Close", _name.c_str());
    bool ret = true;
    MutexLock lock(&_mu, "Close");
    if (_block_for_write && ((_open_flags & O_WRONLY) || _open_flags == O_APPEND)) {
        if (!_write_buf) {
            _write_buf = new WriteBuffer(++_last_seq, 32, _block_for_write->block_id(),
                                         _block_for_write->block_size());
        }
        _write_buf->SetLast();
        StartWrite(_write_buf);

        //common::timer::AutoTimer at(1, "LastWrite", _name.c_str());
        while (_back_writing) {
            _sync_signal.Wait();
        }
        delete _block_for_write;
        _block_for_write = NULL;
    }
    delete _chunkserver;
    _chunkserver = NULL;
    LOG(DEBUG, "File %s closed", _name.c_str());
    _closed = true;
    return ret;
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
