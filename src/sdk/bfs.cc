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

#include "proto/nameserver.pb.h"
#include "proto/chunkserver.pb.h"
#include "rpc/rpc_client.h"
#include <common/atomic.h>
#include <common/mutex.h>
#include <common/timer.h>
#include <common/sliding_window.h>
#include <common/logging.h>
#include <common/string_util.h>
#include <common/tprinter.h>

#include "proto/status_code.pb.h"

#include "bfs.h"

DECLARE_string(nameserver);
DECLARE_string(nameserver_port);
DECLARE_int32(sdk_thread_num);
DECLARE_int32(sdk_file_reada_len);
DECLARE_string(sdk_write_mode);

namespace baidu {
namespace bfs {

ThreadPool* g_thread_pool = NULL;

struct LocatedBlocks {
    int64_t file_length_;
    std::vector<LocatedBlock> blocks_;
    void CopyFrom(const ::google::protobuf::RepeatedPtrField< baidu::bfs::LocatedBlock >& blocks) {
        for (int i = 0; i < blocks.size(); i++) {
            blocks_.push_back(blocks.Get(i));
        }
    }
};

class FSImpl;

class WriteBuffer {
public:
    WriteBuffer(int32_t seq, int32_t buf_size, int64_t block_id, int64_t offset)
        : buf_size_(buf_size), data_size_(0),
          block_id_(block_id), offset_(offset),
          seq_id_(seq), is_last_(false), refs_(0) {
        buf_= new char[buf_size];
    }
    ~WriteBuffer() {
        delete[] buf_;
        buf_ = NULL;
    }
    int Available() {
        return buf_size_ - data_size_;
    }
    int Append(const char* buf, int len) {
        assert(len + data_size_ <= buf_size_);
        memcpy(buf_ + data_size_, buf, len);
        data_size_ += len;
        return data_size_;
    }
    const char* Data() {
        return buf_;
    }
    int Size() const {
        return data_size_;
    }
    int Sequence() const {
        return seq_id_;
    }
    void Clear() {
        data_size_ = 0;
    }
    void SetLast() {
        is_last_ = true;
    }
    bool IsLast() const { return is_last_; }
    int64_t offset() const { return offset_; }
    int64_t block_id() const { return block_id_; }
    void AddRefBy(int counter) {
        common::atomic_add(&refs_, counter);
    }
    void AddRef() {
        common::atomic_inc(&refs_);
        assert (refs_ > 0);
    }
    void DecRef() {
        if (common::atomic_add(&refs_, -1) == 1) {
            assert(refs_ == 0);
            delete this;
        }
    }
private:
    int32_t buf_size_;
    int32_t data_size_;
    char*   buf_;
    int64_t block_id_;
    int64_t offset_;
    int32_t seq_id_;
    bool    is_last_;
    volatile int refs_;
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
    void StartWrite();
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
    bool Sync(int32_t timeout = 0);
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
    FSImpl* fs_;                        ///< 文件系统
    RpcClient* rpc_client_;             ///< RpcClient
    std::string name_;                  ///< 文件路径
    int32_t open_flags_;                ///< 打开使用的flag

    /// for write
    LocatedBlock* block_for_write_;     ///< 正在写的block
    WriteBuffer* write_buf_;            ///< 本地写缓冲
    int32_t last_seq_;                  ///< last sequence number
    std::map<std::string, common::SlidingWindow<int>* > write_windows_;
    std::priority_queue<WriteBuffer*, std::vector<WriteBuffer*>, WriteBufferCmp>
        write_queue_;                   ///< Write buffer list
    volatile int back_writing_;         ///< Async write running backgroud

    /// for read
    LocatedBlocks located_blocks_;      ///< block meta for read
    ChunkServer_Stub* chunkserver_;     ///< located chunkserver
    std::map<std::string, ChunkServer_Stub*> chunkservers_; ///< located chunkservers
    int64_t read_offset_;               ///< 读取的偏移
    char* reada_buffer_;                ///< Read ahead buffer
    int32_t reada_buf_len_;             ///< Read ahead buffer length
    int64_t reada_base_;                ///< Read ahead base offset
    int32_t sequential_ratio_;          ///< Sequential read ratio
    int64_t last_read_offset_;

    bool closed_;                       ///< 是否关闭
    Mutex   mu_;
    CondVar sync_signal_;               ///< _sync_var
    bool bg_error_;                     ///< background write error
    std::map<std::string, bool> cs_errors_;        ///< background write error for each chunkserver
};

class FSImpl : public FS {
public:
    friend class BfsFileImpl;
    FSImpl() : rpc_client_(NULL), nameserver_(NULL) {
    }
    ~FSImpl() {
        delete nameserver_;
        delete rpc_client_;
    }
    bool ConnectNameServer(const char* nameserver) {
        if (nameserver != NULL) {
            nameserver_address_ = nameserver;
        } else {
            nameserver_address_ = FLAGS_nameserver + ":" + FLAGS_nameserver_port;
        }
        rpc_client_ = new RpcClient();
        bool ret = rpc_client_->GetStub(nameserver_address_, &nameserver_);
        return ret;
    }
    bool CreateDirectory(const char* path) {
        CreateFileRequest request;
        CreateFileResponse response;
        request.set_file_name(path);
        request.set_mode(0755|(1<<9));
        request.set_sequence_id(0);
        bool ret = rpc_client_->SendRequest(nameserver_, &NameServer_Stub::CreateFile,
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
        bool ret = rpc_client_->SendRequest(nameserver_, &NameServer_Stub::ListDirectory,
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
        bool ret = rpc_client_->SendRequest(nameserver_, &NameServer_Stub::DeleteDirectory,
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
        bool ret = rpc_client_->SendRequest(nameserver_, &NameServer_Stub::Stat,
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
        bool ret = rpc_client_->SendRequest(nameserver_, &NameServer_Stub::Stat,
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
        FileLocationRequest request;
        FileLocationResponse response;
        request.set_file_name(path);
        request.set_sequence_id(0);
        bool ret = rpc_client_->SendRequest(nameserver_,
            &NameServer_Stub::GetFileLocation, &request, &response, 15, 3);
        if (!ret || response.status() != 0) {
            LOG(WARNING, "GetFileSize(%s) return %d", path, response.status());
            return false;
        }
        *file_size = 0;
        for (int i = 0; i < response.blocks_size(); i++) {
            const LocatedBlock& block = response.blocks(i);
            if (block.block_size()) {
                *file_size += block.block_size();
                continue;
            }
            ChunkServer_Stub* chunkserver = NULL;
            bool available = false;
            for (int j = 0; j < block.chains_size(); j++) {
                std::string addr = block.chains(j).address();
                ret = rpc_client_->GetStub(addr, &chunkserver);
                if (!ret) {
                    LOG(INFO, "GetFileSize(%s) connect chunkserver fail %s",
                        path, addr.c_str());
                } else {
                    GetBlockInfoRequest gbi_request;
                    gbi_request.set_block_id(block.block_id());
                    gbi_request.set_sequence_id(common::timer::get_micros());
                    GetBlockInfoResponse gbi_response;
                    ret = rpc_client_->SendRequest(chunkserver,
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
        return OpenFile(path, flags, 0, -1, file);
    }
    bool OpenFile(const char* path, int32_t flags, int32_t mode,
                  int32_t replica, File** file) {
        common::timer::AutoTimer at(100, "OpenFile", path);
        bool ret = false;
        *file = NULL;
        if (flags & O_WRONLY) {
            CreateFileRequest request;
            CreateFileResponse response;
            request.set_file_name(path);
            request.set_sequence_id(0);
            request.set_flags(flags);
            request.set_mode(mode&0777);
            request.set_replica_num(replica);
            ret = rpc_client_->SendRequest(nameserver_, &NameServer_Stub::CreateFile,
                &request, &response, 15, 3);
            if (!ret || response.status() != 0) {
                LOG(WARNING, "Open file for write fail: %s, status= %d\n",
                    path, response.status());
                ret = false;
            } else {
                //printf("Open file %s\n", path);
                *file = new BfsFileImpl(this, rpc_client_, path, flags);
            }
        } else if (flags == O_RDONLY) {
            FileLocationRequest request;
            FileLocationResponse response;
            request.set_file_name(path);
            request.set_sequence_id(0);
            ret = rpc_client_->SendRequest(nameserver_, &NameServer_Stub::GetFileLocation,
                &request, &response, 15, 3);
            if (ret && response.status() == 0) {
                BfsFileImpl* f = new BfsFileImpl(this, rpc_client_, path, flags);
                f->located_blocks_.CopyFrom(response.blocks());
                *file = f;
                //printf("OpenFile success: %s\n", path);
            } else {
                //printf("GetFileLocation return %d\n", response.blocks_size());
                LOG(WARNING, "OpenFile return %d\n", response.status());
                ret = false;
            }
        } else {
            LOG(WARNING, "Open flags only O_RDONLY or O_WRONLY, but %d", flags);
            ret = false;
        }
        return ret;
    }
    bool CloseFile(File* file) {
        return file->Close();
    }
    bool DeleteFile(const char* path) {
        UnlinkRequest request;
        UnlinkResponse response;
        request.set_path(path);
        int64_t seq = common::timer::get_micros();
        request.set_sequence_id(seq);
        // printf("Delete file: %s\n", path);
        bool ret = rpc_client_->SendRequest(nameserver_, &NameServer_Stub::Unlink,
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
        bool ret = rpc_client_->SendRequest(nameserver_, &NameServer_Stub::Rename,
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
        bool ret = rpc_client_->SendRequest(nameserver_,
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
        bool ret = rpc_client_->SendRequest(nameserver_,
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
    RpcClient* rpc_client_;
    NameServer_Stub* nameserver_;
    std::string nameserver_address_;
};

BfsFileImpl::BfsFileImpl(FSImpl* fs, RpcClient* rpc_client,
                         const std::string name, int32_t flags)
  : fs_(fs), rpc_client_(rpc_client), name_(name),
    open_flags_(flags), block_for_write_(NULL),
    write_buf_(NULL), last_seq_(-1), back_writing_(0),
    chunkserver_(NULL), read_offset_(0), reada_buffer_(NULL),
    reada_buf_len_(0), reada_base_(0), sequential_ratio_(0),
    last_read_offset_(-1), closed_(false),
    sync_signal_(&mu_), bg_error_(false) {
}

BfsFileImpl::~BfsFileImpl () {
    if (!closed_) {
        Close();
    }
    delete[] reada_buffer_;
    reada_buffer_ = NULL;
    std::map<std::string, common::SlidingWindow<int>* >::iterator w_it;
    for (w_it = write_windows_.begin(); w_it != write_windows_.end(); ++w_it) {
        delete w_it->second;
        w_it->second = NULL;
    }
    std::map<std::string, ChunkServer_Stub*>::iterator it;
    for (it = chunkservers_.begin(); it != chunkservers_.end(); ++it) {
        delete it->second;
        it->second = NULL;
    }
}

int32_t BfsFileImpl::Pread(char* buf, int32_t read_len, int64_t offset, bool reada) {
    if (read_len <= 0 || buf == NULL || offset < 0) {
        LOG(WARNING, "Pread(%s, %ld, %d), bad parameters!",
            name_.c_str(), offset, read_len);
        return -1;
    }
    {
        MutexLock lock(&mu_, "Pread read buffer", 1000);
        if (last_read_offset_ == -1
            || last_read_offset_ != offset) {
            sequential_ratio_ /= 2;
            LOG(DEBUG, "Pread(%s, %ld, %d) missing last_offset %ld",
                name_.c_str(), offset, read_len, last_read_offset_);
        } else {
            sequential_ratio_++;
        }
        last_read_offset_ = offset + read_len;
        if (reada_buffer_ && reada_base_ <= offset &&
                reada_base_ + reada_buf_len_ >= offset + read_len) {
            memcpy(buf, reada_buffer_ + (offset - reada_base_), read_len);
            //LOG(INFO, "Read %s %ld from cache %ld", _name.c_str(), offset, read_len);
            return read_len;
        }
    }
    LocatedBlock lcblock;
    ChunkServer_Stub* chunk_server = NULL;
    std::string cs_addr;

    int server_index = 0;
    {
        MutexLock lock(&mu_, "Pread GetStub", 1000);
        if (located_blocks_.blocks_.empty()) {
            return 0;
        } else if (located_blocks_.blocks_[0].chains_size() == 0) {
            LOG(WARNING, "No located servers or located_blocks_[%lu]",
                located_blocks_.blocks_.size());
            return -3;
        }
        lcblock.CopyFrom(located_blocks_.blocks_[0]);
        server_index = rand() % lcblock.chains_size();
        cs_addr = lcblock.chains(server_index).address();
        if (chunkserver_ == NULL) {
            fs_->rpc_client_->GetStub(cs_addr, &chunkserver_);
        }
        chunk_server = chunkserver_;
    }
    int64_t block_id = lcblock.block_id();

    ReadBlockRequest request;
    ReadBlockResponse response;
    request.set_sequence_id(common::timer::get_micros());
    request.set_block_id(block_id);
    request.set_offset(offset);
    int32_t rlen = read_len;
    if (sequential_ratio_ > 2
        && reada
        && read_len < FLAGS_sdk_file_reada_len) {
        rlen = std::min(FLAGS_sdk_file_reada_len, sequential_ratio_ * read_len);
        LOG(DEBUG, "Pread(%s, %ld, %d) sequential_ratio_: %d, readahead to %d",
            name_.c_str(), offset, read_len, sequential_ratio_, rlen);
    }
    request.set_read_len(rlen);
    bool ret = false;

    for (int retry_times = 0; retry_times < lcblock.chains_size() * 2; retry_times++) {
        LOG(DEBUG, "Start Pread: %s", cs_addr.c_str());
        ret = fs_->rpc_client_->SendRequest(chunk_server, &ChunkServer_Stub::ReadBlock,
                    &request, &response, 15, 3);

        if (!ret || response.status() != kOK) {
            ///TODO: Add to _badchunkservers_
            cs_addr = lcblock.chains((++server_index) % lcblock.chains_size()).address();
            LOG(INFO, "Pread retry another chunkserver: %s", cs_addr.c_str());
            fs_->rpc_client_->GetStub(cs_addr, &chunk_server);
            {
                MutexLock lock(&mu_, "Pread change chunkserver_", 1000);
                chunkserver_ = chunk_server;
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
        MutexLock lock(&mu_, "Pread fill buffer", 1000);
        int32_t cache_len = ret_len - read_len;
        if (cache_len > reada_buf_len_) {
            delete[] reada_buffer_;
            reada_buffer_ = new char[cache_len];
        }
        reada_buf_len_ = cache_len;
        memcpy(reada_buffer_, response.databuf().data() + read_len, cache_len);
        reada_base_ = offset + read_len;
        ret_len = read_len;
    }
    assert(read_len >= ret_len);
    memcpy(buf, response.databuf().data(), ret_len);
    return ret_len;
}

int64_t BfsFileImpl::Seek(int64_t offset, int32_t whence) {
    //printf("Seek[%s:%d:%ld]\n", _name.c_str(), whence, offset);
    if (open_flags_ != O_RDONLY) {
        return -2;
    }
    if (whence == SEEK_SET) {
        read_offset_ = offset;
    } else if (whence == SEEK_CUR) {
        common::atomic_add64(&read_offset_, offset);
    } else {
        return -1;
    }
    return read_offset_;
}

int32_t BfsFileImpl::Read(char* buf, int32_t read_len) {
    //LOG(DEBUG, "[%p] Read[%s:%ld] offset= %ld\n",
    //    this, _name.c_str(), read_len, read_offset_);
    if (open_flags_ != O_RDONLY) {
        return -2;
    }
    int32_t ret = Pread(buf, read_len, read_offset_, true);
    //LOG(INFO, "Read[%s:%ld,%ld] return %d", _name.c_str(), read_offset_, read_len, ret);
    if (ret >= 0) {
        common::atomic_add64(&read_offset_, ret);
    }
    return ret;
}

int32_t BfsFileImpl::Write(const char* buf, int32_t len) {
    common::timer::AutoTimer at(100, "Write", name_.c_str());

    {
        MutexLock lock(&mu_, "Write", 1000);
        if (!(open_flags_ & O_WRONLY)) {
            return -2;
        } else if (bg_error_) {
            return -3;
        } else if (closed_) {
            return -4;
        }
        common::atomic_inc(&back_writing_);
    }
    if (open_flags_ & O_WRONLY) {
        MutexLock lock(&mu_, "Write AddBlock", 1000);
        // Add block
        if (chunkservers_.empty()) {
            AddBlockRequest request;
            AddBlockResponse response;
            request.set_sequence_id(0);
            request.set_file_name(name_);
            bool ret = rpc_client_->SendRequest(fs_->nameserver_, &NameServer_Stub::AddBlock,
                &request, &response, 15, 3);
            if (!ret || !response.has_block()) {
                LOG(WARNING, "AddBlock fail for %s\n", name_.c_str());
                common::atomic_dec(&back_writing_);
                return -1;
            }
            block_for_write_ = new LocatedBlock(response.block());
            int cs_size = FLAGS_sdk_write_mode == "chains" ? 1 :
                                                    block_for_write_->chains_size();
            for (int i = 0; i < cs_size; i++) {
                const std::string& addr = block_for_write_->chains(i).address();
                rpc_client_->GetStub(addr, &chunkservers_[addr]);
                write_windows_[addr] = new common::SlidingWindow<int>(100,
                                       boost::bind(&BfsFileImpl::OnWriteCommit, this, _1, _2));
                cs_errors_[addr] = false;
            }
        }
    }

    int32_t w = 0;
    while (w < len) {
        MutexLock lock(&mu_, "WriteInternal", 1000);
        if (write_buf_ == NULL) {
            write_buf_ = new WriteBuffer(++last_seq_, 256*1024,
                                         block_for_write_->block_id(),
                                         block_for_write_->block_size());
        }
        if ( (len - w) < write_buf_->Available()) {
            write_buf_->Append(buf+w, len-w);
            w = len;
            break;
        } else {
            int n = write_buf_->Available();
            write_buf_->Append(buf+w, n);
            w += n;
        }
        if (write_buf_->Available() == 0) {
            StartWrite();
        }
    }
    // printf("Write return %d, buf_size=%d\n", w, file->write_buf_->Size());
    common::atomic_dec(&back_writing_);
    return w;
}

void BfsFileImpl::StartWrite() {
    common::timer::AutoTimer at(5, "StartWrite", name_.c_str());
    mu_.AssertHeld();
    write_queue_.push(write_buf_);
    block_for_write_->set_block_size(block_for_write_->block_size() + write_buf_->Size());
    write_buf_ = NULL;
    boost::function<void ()> task =
        boost::bind(&BfsFileImpl::BackgroundWrite, this);
    common::atomic_inc(&back_writing_);
    mu_.Unlock();
    g_thread_pool->AddTask(task);
    mu_.Lock("StartWrite relock", 1000);
}

bool BfsFileImpl::CheckWriteWindows() {
    mu_.AssertHeld();
    if (FLAGS_sdk_write_mode == "chains") {
        return write_windows_.begin()->second->UpBound() > write_queue_.top()->Sequence();
    }
    std::map<std::string, common::SlidingWindow<int>* >::iterator it;
    int count = 0;
    for (it = write_windows_.begin(); it != write_windows_.end(); ++it) {
        if (it->second->UpBound() > write_queue_.top()->Sequence()) {
            count++;
        }
    }
    return count >= (int)write_windows_.size() - 1;
}

/// Send local buffer to chunkserver
void BfsFileImpl::BackgroundWrite() {
    MutexLock lock(&mu_, "BackgroundWrite", 1000);
    while(!write_queue_.empty() && CheckWriteWindows()) {
        WriteBuffer* buffer = write_queue_.top();
        write_queue_.pop();
        mu_.Unlock();

        buffer->AddRefBy(chunkservers_.size());
        for (size_t i = 0; i < chunkservers_.size(); i++) {
            std::string cs_addr = block_for_write_->chains(i).address();
            bool delay = false;
            if (!(write_windows_[cs_addr]->UpBound() > write_queue_.top()->Sequence())) {
                delay = true;
            }
            {
                // skip bad chunkserver
                ///TODO improve here?
                MutexLock lock(&mu_);
                if (cs_errors_[cs_addr]) {
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
                for (int i = 1; i < block_for_write_->chains_size(); i++) {
                    std::string addr = block_for_write_->chains(i).address();
                    request->add_chunkservers(addr);
                }
            }
            const int max_retry_times = 5;
            ChunkServer_Stub* stub = chunkservers_[cs_addr];
            boost::function<void (const WriteBlockRequest*, WriteBlockResponse*, bool, int)> callback
                = boost::bind(&BfsFileImpl::WriteChunkCallback, this, _1, _2, _3, _4,
                        max_retry_times, buffer, cs_addr);

            LOG(DEBUG, "BackgroundWrite start [bid:%ld, seq:%d, offset:%ld, len:%d]\n",
                    buffer->block_id(), buffer->Sequence(), buffer->offset(), buffer->Size());
            common::atomic_inc(&back_writing_);
            if (delay) {
                g_thread_pool->DelayTask(5,
                        boost::bind(&BfsFileImpl::DelayWriteChunk, this, buffer,
                            request, max_retry_times, cs_addr));
            } else {
                rpc_client_->AsyncRequest(stub, &ChunkServer_Stub::WriteBlock,
                        request, response, callback, 60, 1);
            }
        }
        mu_.Lock("BackgroundWriteRelock", 1000);
    }
    common::atomic_dec(&back_writing_);    // for AddTask
}

void BfsFileImpl::DelayWriteChunk(WriteBuffer* buffer,
                                  const WriteBlockRequest* request,
                                  int retry_times, std::string cs_addr) {
    WriteBlockResponse* response = new WriteBlockResponse;
    boost::function<void (const WriteBlockRequest*, WriteBlockResponse*, bool, int)> callback
        = boost::bind(&BfsFileImpl::WriteChunkCallback, this, _1, _2, _3, _4,
                      retry_times, buffer, cs_addr);
    common::atomic_inc(&back_writing_);
    ChunkServer_Stub* stub = chunkservers_[cs_addr];
    rpc_client_->AsyncRequest(stub, &ChunkServer_Stub::WriteBlock,
        request, response, callback, 60, 1);

    common::atomic_dec(&back_writing_);    // for DelayTask
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
                    name_.c_str(),
                    buffer->block_id(), buffer->Sequence(),
                    buffer->offset(), buffer->Size(),
                    response->status(), retry_times);
            }
            if (--retry_times == 0) {
                LOG(WARNING, "BackgroundWrite error %s"
                    "#%ld seq:%d, offset:%ld, len:%d]"
                    " status: %d, retry_times: %d",
                    name_.c_str(),
                    buffer->block_id(), buffer->Sequence(),
                    buffer->offset(), buffer->Size(),
                    response->status(), retry_times);
                ///TODO: SetFaild & handle it
                if (FLAGS_sdk_write_mode == "chains") {
                    bg_error_ = true;
                } else {
                    MutexLock lock(&mu_);
                    cs_errors_[cs_addr] = true;
                    std::map<std::string, bool>::iterator it = cs_errors_.begin();
                    int count = 0;
                    for (; it != cs_errors_.end(); ++it) {
                        if (it->second == true) {
                            count++;
                        }
                    }
                    if (count > 1) {
                        bg_error_ = true;
                    }
                }
                buffer->DecRef();
                delete request;
            }
        }
        if (!bg_error_) {
            common::atomic_inc(&back_writing_);
            g_thread_pool->DelayTask(5,
                boost::bind(&BfsFileImpl::DelayWriteChunk, this, buffer,
                            request, retry_times, cs_addr));
        }
    } else {
        LOG(DEBUG, "BackgroundWrite done bid:%ld, seq:%d, offset:%ld, len:%d, back_writing_:%d",
            buffer->block_id(), buffer->Sequence(), buffer->offset(),
            buffer->Size(), back_writing_);
        int r = write_windows_[cs_addr]->Add(buffer->Sequence(), 0);
        assert(r == 0);
        buffer->DecRef();
        delete request;
    }
    delete response;

    {
        MutexLock lock(&mu_, "WriteChunkCallback", 1000);
        if (write_queue_.empty() || bg_error_) {
            common::atomic_dec(&back_writing_);    // for AsyncRequest
            if (back_writing_ == 0) {
                sync_signal_.Broadcast();
            }
            return;
        }
    }

    boost::function<void ()> task =
        boost::bind(&BfsFileImpl::BackgroundWrite, this);
    g_thread_pool->AddTask(task);
}

void BfsFileImpl::OnWriteCommit(int32_t, int) {
}

bool BfsFileImpl::Flush() {
    // Not implement
    return true;
}
bool BfsFileImpl::Sync(int32_t timeout) {
    common::timer::AutoTimer at(50, "Sync", name_.c_str());
    if (open_flags_ != O_WRONLY) {
        return false;
    }
    MutexLock lock(&mu_, "Sync", 1000);
    if (write_buf_ && write_buf_->Size()) {
        StartWrite();
    }
    int wait_time = 0;
    while (back_writing_ && !bg_error_ && (timeout == 0 || wait_time < timeout)) {
        bool finish = sync_signal_.TimeWait(1000, "Sync wait");
        if (++wait_time >= 30 && (wait_time % 10 == 0)) {
            LOG(WARNING, "Sync timeout %d s, %s back_writing_= %d, finish= %d",
                wait_time, name_.c_str(), back_writing_, finish);
        }
    }
    // fprintf(stderr, "Sync %s fail\n", _name.c_str());
    return !bg_error_ && !back_writing_;
}

bool BfsFileImpl::Close() {
    common::timer::AutoTimer at(500, "Close", name_.c_str());
    MutexLock lock(&mu_, "Close", 1000);
    bool need_report_finish = false;
    int64_t block_id = -1;
    if (block_for_write_ && (open_flags_ & O_WRONLY)) {
        need_report_finish = true;
        block_id = block_for_write_->block_id();
        if (!write_buf_) {
            write_buf_ = new WriteBuffer(++last_seq_, 32, block_id,
                                         block_for_write_->block_size());
        }
        write_buf_->SetLast();
        StartWrite();

        //common::timer::AutoTimer at(1, "LastWrite", _name.c_str());
        int wait_time = 0;
        while (back_writing_) {
            bool finish = sync_signal_.TimeWait(1000, (name_ + " Close wait").c_str());
            if (++wait_time >= 30 && (wait_time % 10 == 0)) {
                LOG(WARNING, "Close timeout %d s, %s back_writing_= %d, finish= %d",
                wait_time, name_.c_str(), back_writing_, finish);
            }
        }
        delete block_for_write_;
        block_for_write_ = NULL;
    }
    delete chunkserver_;
    chunkserver_ = NULL;
    LOG(DEBUG, "File %s closed", name_.c_str());
    closed_ = true;
    bool ret = true;
    if (bg_error_) {
        LOG(WARNING, "Close file %s fail", name_.c_str());
        ret = false;
    } else if (need_report_finish) {
        NameServer_Stub* nameserver = fs_->nameserver_;
        FinishBlockRequest request;
        FinishBlockResponse response;
        request.set_sequence_id(0);
        request.set_block_id(block_id);
        request.set_block_version(last_seq_);
        ret = rpc_client_->SendRequest(nameserver, &NameServer_Stub::FinishBlock,
                &request, &response, 15, 3);
        if (!(ret && response.status() == 0 && (!bg_error_)))  {
            LOG(WARNING, "Close file fail: %s", name_.c_str());
            ret = false;
        }
    }
    return ret;
}

bool FS::OpenFileSystem(const char* nameserver, FS** fs) {
    FSImpl* impl = new FSImpl;
    if (!impl->ConnectNameServer(nameserver)) {
        *fs = NULL;
        return false;
    }
    *fs = impl;
    g_thread_pool = new ThreadPool(FLAGS_sdk_thread_num);
    g_thread_pool->Start();
    return true;
}

} // namespace bfs
} // namespace baidu

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
