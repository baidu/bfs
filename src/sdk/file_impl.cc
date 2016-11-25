// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
#include "file_impl.h"

#include <functional>

#include <gflags/gflags.h>
#include <common/sliding_window.h>
#include <common/logging.h>

#include "proto/status_code.pb.h"
#include "rpc/rpc_client.h"
#include "rpc/nameserver_client.h"

#include "fs_impl.h"

DECLARE_int32(sdk_file_reada_len);
DECLARE_int32(sdk_createblock_retry);
DECLARE_int32(sdk_write_retry_times);


namespace baidu {
namespace bfs {

WriteBuffer::WriteBuffer(int32_t seq, int32_t buf_size, int64_t block_id, int64_t offset)
    : buf_size_(buf_size), data_size_(0),
      block_id_(block_id), offset_(offset),
      seq_id_(seq), is_last_(false), refs_(0) {
    buf_= new char[buf_size];
}
WriteBuffer::~WriteBuffer() {
    delete[] buf_;
    buf_ = NULL;
}
int WriteBuffer::Available() {
    return buf_size_ - data_size_;
}
int WriteBuffer::Append(const char* buf, int len) {
    assert(len + data_size_ <= buf_size_);
    memcpy(buf_ + data_size_, buf, len);
    data_size_ += len;
    return data_size_;
}
const char* WriteBuffer::Data() {
    return buf_;
}
int WriteBuffer::Size() const {
    return data_size_;
}
int WriteBuffer::Sequence() const {
    return seq_id_;
}
void WriteBuffer::Clear() {
    data_size_ = 0;
}
void WriteBuffer::SetLast() {
    is_last_ = true;
}
bool WriteBuffer::IsLast() const {
    return is_last_;
}
int64_t WriteBuffer::offset() const {
    return offset_;
}
int64_t WriteBuffer::block_id() const {
    return block_id_;
}
void WriteBuffer::AddRefBy(int counter) {
    common::atomic_add(&refs_, counter);
}
void WriteBuffer::AddRef() {
    common::atomic_inc(&refs_);
    assert (refs_ > 0);
}
void WriteBuffer::DecRef() {
    if (common::atomic_add(&refs_, -1) == 1) {
        assert(refs_ == 0);
        delete this;
    }
}

FileImpl::FileImpl(FSImpl* fs, RpcClient* rpc_client,
                   const std::string& name, int32_t flags, const WriteOptions& options)
  : fs_(fs), rpc_client_(rpc_client), name_(name),
    open_flags_(flags), write_offset_(0), block_for_write_(NULL),
    write_buf_(NULL), last_seq_(-1), back_writing_(0),
    w_options_(options),
    chunkserver_(NULL), last_chunkserver_index_(-1),
    read_offset_(0), reada_buffer_(NULL),
    reada_buf_len_(0), reada_base_(0), sequential_ratio_(0),
    last_read_offset_(-1), r_options_(ReadOptions()), closed_(false), synced_(false),
    sync_signal_(&mu_), bg_error_(false) {
        thread_pool_ = fs->thread_pool_;
}

FileImpl::FileImpl(FSImpl* fs, RpcClient* rpc_client,
                   const std::string& name, int32_t flags, const ReadOptions& options)
  : fs_(fs), rpc_client_(rpc_client), name_(name),
    open_flags_(flags), write_offset_(0), block_for_write_(NULL),
    write_buf_(NULL), last_seq_(-1), back_writing_(0),
    w_options_(WriteOptions()),
    chunkserver_(NULL), last_chunkserver_index_(-1),
    read_offset_(0), reada_buffer_(NULL),
    reada_buf_len_(0), reada_base_(0), sequential_ratio_(0),
    last_read_offset_(-1), r_options_(options), closed_(false), synced_(false),
    sync_signal_(&mu_), bg_error_(false) {
        thread_pool_ = fs->thread_pool_;
}

FileImpl::~FileImpl () {
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
    std::set<ChunkServer_Stub*>::iterator bad_cs_it;
    for (bad_cs_it = bad_chunkservers_.begin();
            bad_cs_it != bad_chunkservers_.end(); ++bad_cs_it) {
        delete *bad_cs_it;
    }
}

int32_t FileImpl::Pread(char* buf, int32_t read_len, int64_t offset, bool reada) {
    if (read_len <= 0 || buf == NULL || offset < 0) {
        LOG(WARNING, "Pread(%s, %ld, %d), bad parameters!",
            name_.c_str(), offset, read_len);
        return BAD_PARAMETER;
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
    int64_t block_id;
    {
        MutexLock lock(&mu_, "Pread GetStub", 1000);
        if (located_blocks_.blocks_.empty()) {
            return 0;
        } else if (located_blocks_.blocks_[0].chains_size() == 0) {
            if (located_blocks_.blocks_[0].block_size() == 0) {
                return 0;
            } else {
                LOG(WARNING, "No located chunkserver of block #%ld",
                    located_blocks_.blocks_[0].block_id());
                return TIMEOUT;
            }
        }
        lcblock.CopyFrom(located_blocks_.blocks_[0]);
        if (last_chunkserver_index_ == -1 || !chunkserver_) {
            const std::string& local_host_name = fs_->local_host_name_;
            for (int i = 0; i < lcblock.chains_size(); i++) {
                std::string addr = lcblock.chains(i).address();
                std::string cs_name = std::string(addr, 0, addr.find_last_of(':'));
                if (cs_name == local_host_name) {
                    last_chunkserver_index_ = i;
                    cs_addr = lcblock.chains(i).address();
                    break;
                }
            }
            if (last_chunkserver_index_ == -1) {
                int server_index = rand() % lcblock.chains_size();
                cs_addr = lcblock.chains(server_index).address();
                last_chunkserver_index_ = server_index;
            }
            fs_->rpc_client_->GetStub(cs_addr, &chunkserver_);
        }
        chunk_server = chunkserver_;
        block_id = lcblock.block_id();
    }

    ReadBlockRequest request;
    ReadBlockResponse response;
    request.set_sequence_id(common::timer::get_micros());
    request.set_block_id(block_id);
    request.set_offset(offset);
    int32_t rlen = read_len;
    if (sequential_ratio_ > 2
        && reada
        && read_len < FLAGS_sdk_file_reada_len) {
        rlen = std::min(static_cast<int64_t>(FLAGS_sdk_file_reada_len),
                        static_cast<int64_t>(sequential_ratio_) * read_len);
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
            cs_addr = lcblock.chains((++last_chunkserver_index_) % lcblock.chains_size()).address();
            LOG(INFO, "Pread retry another chunkserver: %s", cs_addr.c_str());
            {
                MutexLock lock(&mu_, "Pread change chunkserver", 1000);
                if (chunk_server != chunkserver_) {
                    chunk_server = chunkserver_;
                } else {
                    bad_chunkservers_.insert(chunk_server);
                    fs_->rpc_client_->GetStub(cs_addr, &chunk_server);
                    chunkserver_ = chunk_server;
                }
            }
        } else {
            break;
        }
    }

    if (!ret || response.status() != kOK) {
        LOG(WARNING, "Read block %ld fail, ret= %d status= %s\n", block_id, ret, StatusCode_Name(response.status()).c_str());
        if (!ret) {
            return TIMEOUT;
        } else {
            return GetErrorCode(response.status());
        }
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

int64_t FileImpl::Seek(int64_t offset, int32_t whence) {
    //printf("Seek[%s:%d:%ld]\n", _name.c_str(), whence, offset);
    if (open_flags_ != O_RDONLY) {
        if (offset == 0 && whence == SEEK_CUR) {
            return common::atomic_add64(&write_offset_, 0);
        }
        return BAD_PARAMETER;
    }
    MutexLock lock(&read_offset_mu_);
    if (whence == SEEK_SET) {
        read_offset_ = offset;
    } else if (whence == SEEK_CUR) {
        read_offset_ += offset;
    } else {
        return BAD_PARAMETER;
    }
    return read_offset_;
}

int32_t FileImpl::Read(char* buf, int32_t read_len) {
    //LOG(DEBUG, "[%p] Read[%s:%ld] offset= %ld\n",
    //    this, _name.c_str(), read_len, read_offset_);
    if (open_flags_ != O_RDONLY) {
        return BAD_PARAMETER;
    }
    MutexLock lock(&read_offset_mu_);
    int32_t ret = Pread(buf, read_len, read_offset_, true);
    //LOG(INFO, "Read[%s:%ld,%ld] return %d", _name.c_str(), read_offset_, read_len, ret);
    if (ret >= 0) {
        read_offset_ += ret;
    }
    return ret;
}

int32_t FileImpl::AddBlock() {
    AddBlockRequest request;
    AddBlockResponse response;
    request.set_sequence_id(0);
    request.set_file_name(name_);
    const std::string& local_host_name = fs_->local_host_name_;
    request.set_client_address(local_host_name);
    bool ret = fs_->nameserver_client_->SendRequest(&NameServer_Stub::AddBlock,
                                                    &request, &response, 15, 1);
    if (!ret || !response.has_block()) {
        LOG(WARNING, "Nameserver AddBlock fail: %s, ret= %d, status= %s",
            name_.c_str(), ret, StatusCode_Name(response.status()).c_str());
        if (!ret) {
            return TIMEOUT;
        } else {
            return GetErrorCode(response.status());
        }
    }
    block_for_write_ = new LocatedBlock(response.block());
    bool chains_write = IsChainsWrite();
    int cs_size = chains_write ? 1 : block_for_write_->chains_size();
    for (int i = 0; i < cs_size; i++) {
        const std::string& addr = block_for_write_->chains(i).address();
        rpc_client_->GetStub(addr, &chunkservers_[addr]);
        write_windows_[addr] = new common::SlidingWindow<int>(100,
                               std::bind(&FileImpl::OnWriteCommit,
                                std::placeholders::_1, std::placeholders::_2));
        cs_errors_[addr] = false;
        WriteBlockRequest create_request;
        int64_t seq = common::timer::get_micros();
        create_request.set_sequence_id(seq);
        create_request.set_block_id(block_for_write_->block_id());
        create_request.set_databuf("", 0);
        create_request.set_offset(0);
        create_request.set_is_last(false);
        create_request.set_packet_seq(0);
        WriteBlockResponse create_response;
        if (chains_write) {
            for (int i = 0; i < block_for_write_->chains_size(); i++) {
                const std::string& cs_addr = block_for_write_->chains(i).address();
                create_request.add_chunkservers(cs_addr);
            }
        }
        bool ret = rpc_client_->SendRequest(chunkservers_[addr],
                                            &ChunkServer_Stub::WriteBlock,
                                            &create_request, &create_response,
                                            25, 1);
        if (!ret || create_response.status() != 0) {
            LOG(WARNING, "Chunkserver AddBlock fail: %s ret=%d status= %s",
                name_.c_str(), ret, StatusCode_Name(create_response.status()).c_str());
            for (int j = 0; j <= i; j++) {
                const std::string& cs_addr = block_for_write_->chains(j).address();
                delete write_windows_[cs_addr];
                delete chunkservers_[cs_addr];
            }
            write_windows_.clear();
            chunkservers_.clear();
            delete block_for_write_;
            block_for_write_ = NULL;
            if (!ret) {
                return TIMEOUT;
            } else {
                return GetErrorCode(create_response.status());
            }
        }
        write_windows_[addr]->Add(0, 0);
    }
    last_seq_ = 0;
    return OK;
}
int32_t FileImpl::Write(const char* buf, int32_t len) {
    common::timer::AutoTimer at(100, "Write", name_.c_str());

    {
        MutexLock lock(&mu_, "Write", 1000);
        if (!(open_flags_ & O_WRONLY)) {
            return BAD_PARAMETER;
        } else if (bg_error_) {
            return TIMEOUT;
        } else if (closed_) {
            return BAD_PARAMETER;
        }
        common::atomic_inc(&back_writing_);
    }
    if (open_flags_ & O_WRONLY) {
        // Add block
        MutexLock lock(&mu_, "Write AddBlock", 1000);
        if (chunkservers_.empty()) {
            int ret = 0;
            for (int i = 0; i < FLAGS_sdk_createblock_retry; i++) {
                ret = AddBlock();
                if (ret == kOK) break;
                sleep(10);
            }
            if (ret != kOK) {
                LOG(WARNING, "AddBlock fail for %s\n", name_.c_str());
                common::atomic_dec(&back_writing_);
                return ret;
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
    common::atomic_add64(&write_offset_, w);
    common::atomic_dec(&back_writing_);
    return w;
}

void FileImpl::StartWrite() {
    common::timer::AutoTimer at(5, "StartWrite", name_.c_str());
    mu_.AssertHeld();
    write_queue_.push(write_buf_);
    block_for_write_->set_block_size(block_for_write_->block_size() + write_buf_->Size());
    write_buf_ = NULL;
    std::function<void ()> task =
        std::bind(&FileImpl::BackgroundWrite, std::weak_ptr<FileImpl>(shared_from_this()));
    common::atomic_inc(&back_writing_);
    mu_.Unlock();
    thread_pool_->AddTask(task);
    mu_.Lock("StartWrite relock", 1000);
}

bool FileImpl::CheckWriteWindows() {
    mu_.AssertHeld();
    if (IsChainsWrite()) {
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

int32_t FileImpl::FinishedNum() {
    mu_.AssertHeld();
    std::map<std::string, common::SlidingWindow<int>* >::iterator it;
    int count = 0;
    for (it = write_windows_.begin(); it != write_windows_.end(); ++it) {
        if (it->second->GetBaseOffset() == last_seq_ + 1) {
            count++;
        }
    }
    return count;
}

bool FileImpl::ShouldSetError() {
    mu_.AssertHeld();
    std::map<std::string, bool>::iterator it = cs_errors_.begin();
    int count = 0;
    for (; it != cs_errors_.end(); ++it) {
        if (it->second == true) {
            count++;
        }
    }
    if (IsChainsWrite()) {
        return count == 1;
    } else {
        return count > 1;
    }
}

/// Send local buffer to chunkserver
void FileImpl::BackgroundWrite(std::weak_ptr<FileImpl> wk_fp) {
    std::shared_ptr<FileImpl> fp(wk_fp.lock());
    if (!fp) {
        LOG(DEBUG, "FileImpl has been destroied, ignore backgroud write");
        return;
    }
    fp->BackgroundWriteInternal();
}

void FileImpl::BackgroundWriteInternal() {
    MutexLock lock(&mu_, "BackgroundWrite", 1000);
    while(!write_queue_.empty() && CheckWriteWindows()) {
        WriteBuffer* buffer = write_queue_.top();
        write_queue_.pop();
        mu_.Unlock();

        buffer->AddRefBy(chunkservers_.size());
        for (size_t i = 0; i < chunkservers_.size(); i++) {
            std::string cs_addr = block_for_write_->chains(i).address();
            bool delay = false;
            if (write_windows_[cs_addr]->UpBound() < buffer->Sequence()) {
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
            if (IsChainsWrite()) {
                for (int i = 0; i < block_for_write_->chains_size(); i++) {
                    std::string addr = block_for_write_->chains(i).address();
                    request->add_chunkservers(addr);
                }
            }
            const int max_retry_times = FLAGS_sdk_write_retry_times;
            ChunkServer_Stub* stub = chunkservers_[cs_addr];
            std::function<void (const WriteBlockRequest*, WriteBlockResponse*, bool, int)> callback
                = std::bind(&FileImpl::WriteBlockCallback,
                        std::weak_ptr<FileImpl>(shared_from_this()),
                        std::placeholders::_1, std::placeholders::_2,
                        std::placeholders::_3, std::placeholders::_4,
                        max_retry_times, buffer, cs_addr);

            LOG(DEBUG, "BackgroundWrite start [bid:%ld, seq:%d, offset:%ld, len:%d]\n",
                    buffer->block_id(), buffer->Sequence(), buffer->offset(), buffer->Size());
            common::atomic_inc(&back_writing_);
            if (delay) {
                thread_pool_->DelayTask(5,
                        std::bind(&FileImpl::DelayWriteChunk,
                            std::weak_ptr<FileImpl>(shared_from_this()),
                            buffer, request, max_retry_times, cs_addr));
            } else {
                WriteBlockResponse* response = new WriteBlockResponse;
                rpc_client_->AsyncRequest(stub, &ChunkServer_Stub::WriteBlock,
                        request, response, callback, 60, 1);
            }
        }
        mu_.Lock("BackgroundWriteRelock", 1000);
    }
    common::atomic_dec(&back_writing_);    // for AddTask
    if (back_writing_ == 0) {
        sync_signal_.Broadcast();
    }
}

void FileImpl::DelayWriteChunk(std::weak_ptr<FileImpl> wk_fp,
                               WriteBuffer* buffer,
                               const WriteBlockRequest* request,
                               int retry_times, std::string cs_addr) {
    std::shared_ptr<FileImpl> fp(wk_fp.lock());
    if (!fp) {
        LOG(DEBUG, "FileImpl has been destroied, ignore delay write");
        buffer->DecRef();
        delete request;
        return;
    }
    fp->DelayWriteChunkInternal(buffer, request, retry_times, cs_addr);
}

void FileImpl::DelayWriteChunkInternal(WriteBuffer* buffer,
                               const WriteBlockRequest* request,
                               int retry_times, std::string cs_addr) {
    WriteBlockResponse* response = new WriteBlockResponse;
    std::function<void (const WriteBlockRequest*, WriteBlockResponse*, bool, int)> callback
        = std::bind(&FileImpl::WriteBlockCallback,
                      std::weak_ptr<FileImpl>(shared_from_this()),
                      std::placeholders::_1, std::placeholders::_2,
                      std::placeholders::_3, std::placeholders::_4,
                      retry_times, buffer, cs_addr);
    common::atomic_inc(&back_writing_);
    ChunkServer_Stub* stub = chunkservers_[cs_addr];
    rpc_client_->AsyncRequest(stub, &ChunkServer_Stub::WriteBlock,
        request, response, callback, 60, 1);

    int ret = common::atomic_add(&back_writing_, -1);    // for DelayTask
    if (ret == 1) {
        sync_signal_.Broadcast();
    }
}

void FileImpl::WriteBlockCallback(std::weak_ptr<FileImpl> wk_fp,
                                  const WriteBlockRequest* request,
                                  WriteBlockResponse* response,
                                  bool failed, int error,
                                  int retry_times,
                                  WriteBuffer* buffer,
                                  std::string cs_addr) {
    std::shared_ptr<FileImpl> fp(wk_fp.lock());
    if (!fp) {
        LOG(DEBUG, "FileImpl has been destroied, ignore this callback");
        buffer->DecRef();
        delete request;
        delete response;
        return;
    }
    fp->WriteBlockCallbackInternal(request, response, failed, error, retry_times, buffer, cs_addr);
}

void FileImpl::WriteBlockCallbackInternal(const WriteBlockRequest* request,
                                     WriteBlockResponse* response,
                                     bool failed, int error,
                                     int retry_times,
                                     WriteBuffer* buffer,
                                     std::string cs_addr) {
    if (failed || response->status() != kOK) {
        if (sofa::pbrpc::RPC_ERROR_SEND_BUFFER_FULL != error
                && response->status() != kCsTooMuchPendingBuffer
                && response->status() != kCsTooMuchUnfinishedWrite) {
            if (retry_times < FLAGS_sdk_write_retry_times) {
                LOG(INFO, "BackgroundWrite failed %s"
                    " #%ld seq:%d, offset:%ld, len:%d"
                    " status: %s, retry_times: %d",
                    name_.c_str(),
                    buffer->block_id(), buffer->Sequence(),
                    buffer->offset(), buffer->Size(),
                    StatusCode_Name(response->status()).c_str(), retry_times);
            }
            if (--retry_times == 0) {
                LOG(WARNING, "BackgroundWrite error %s"
                    " #%ld seq:%d, offset:%ld, len:%d"
                    " status: %s, retry_times: %d",
                    name_.c_str(),
                    buffer->block_id(), buffer->Sequence(),
                    buffer->offset(), buffer->Size(),
                    StatusCode_Name(response->status()).c_str(), retry_times);
                ///TODO: SetFaild & handle it
                MutexLock lock(&mu_);
                cs_errors_[cs_addr] = true;
                if (ShouldSetError()) {
                    bg_error_ = true;
                }
            }
        }
        if (!bg_error_ && retry_times > 0) {
            common::atomic_inc(&back_writing_);
            thread_pool_->DelayTask(5000,
                std::bind(&FileImpl::DelayWriteChunk,
                    std::weak_ptr<FileImpl>(shared_from_this()),
                    buffer, request, retry_times, cs_addr));
        } else {
            buffer->DecRef();
            delete request;
        }
    } else {
        LOG(DEBUG, "BackgroundWrite done bid:%ld, seq:%d, offset:%ld, len:%d, back_writing_:%d",
            buffer->block_id(), buffer->Sequence(), buffer->offset(),
            buffer->Size(), back_writing_);
        int64_t diff = common::timer::get_micros() - request->sequence_id();
        if (diff > 200000) {
            LOG(INFO, "Write %s #%ld request use %.3f ms ",
                name_.c_str(), request->block_id(), diff / 1000.0);
        }
        int r = write_windows_[cs_addr]->Add(buffer->Sequence(), 0);
        assert(r == 0);
        buffer->DecRef();
        delete request;
    }
    delete response;

    {
        MutexLock lock(&mu_, "WriteBlockCallback", 1000);
        if (write_queue_.empty() ||
                bg_error_ || EnoughReplica()) {
            common::atomic_dec(&back_writing_);    // for AsyncRequest
            sync_signal_.Broadcast();
            return;
        }
    }

    std::function<void ()> task =
        std::bind(&FileImpl::BackgroundWrite, std::weak_ptr<FileImpl>(shared_from_this()));
    thread_pool_->AddTask(task);
}

void FileImpl::OnWriteCommit(int32_t, int) {
}

int32_t FileImpl::Flush() {
    // Not implement
    return 0;
}
int32_t FileImpl::Sync() {
    common::timer::AutoTimer at(50, "Sync", name_.c_str());
    if (open_flags_ != O_WRONLY) {
        return BAD_PARAMETER;
    }
    MutexLock lock(&mu_, "Sync", 1000);
    int64_t sync_offset = write_offset_;
    if (write_buf_ && write_buf_->Size()) {
        StartWrite();
    }
    int wait_time = 0;
    int32_t replica_num = write_windows_.size();
    if (replica_num == 0) {
        // no need for writing
        return 0;
    }
    int32_t last_write_finish_num = FinishedNum();
    bool chains_write = IsChainsWrite();
    while (!bg_error_ &&
           (w_options_.sync_timeout < 0 || wait_time < w_options_.sync_timeout)) {
        if (last_write_finish_num == replica_num) {
            break;
        }
        //TODO deal with sync_timeout?
        if ((!chains_write && last_write_finish_num == replica_num - 1) && wait_time >= 500) {
            break;
        }
        bool finish = sync_signal_.TimeWait(100, "Sync wait");
        wait_time += 100;
        if (wait_time >= 30000 && (wait_time % 10000 == 0)) {
            LOG(WARNING, "Sync w_options_.sync_timeout %d ms, %s back_writing_= %d, finish= %d",
                wait_time, name_.c_str(), back_writing_, finish);
        }
        last_write_finish_num = FinishedNum();
    }
    if (bg_error_ || !EnoughReplica()) {
        LOG(WARNING, "Sync %s timeout bg_error_ = %d, last_write_finish_num = %d, back_writing_ = %d",
                      name_.c_str(), bg_error_, last_write_finish_num, back_writing_);
        return TIMEOUT;
    }
    if (block_for_write_ && !bg_error_ && sync_offset && !synced_) {
        SyncBlockRequest request;
        SyncBlockResponse response;
        request.set_sequence_id(common::timer::get_micros());
        request.set_block_id(block_for_write_->block_id());
        request.set_file_name(name_);
        request.set_size(sync_offset);
        bool rpc_ret = fs_->nameserver_client_->SendRequest(&NameServer_Stub::SyncBlock,
                                                            &request, &response, 15, 1);
        if (!(rpc_ret && response.status() == kOK))  {
            LOG(WARNING, "Starting file %s fail, starting report returns %d, status: %s",
                    name_.c_str(), rpc_ret, StatusCode_Name(response.status()).c_str());
            if (!rpc_ret) {
                return TIMEOUT;
            } else {
                return GetErrorCode(response.status());
            }
        }
        synced_ = true;
    }
    return OK;
}

int32_t FileImpl::Close() {
    common::timer::AutoTimer at(500, "Close", name_.c_str());
    MutexLock lock(&mu_, "Close", 1000);
    bool need_report_finish = false;
    int64_t block_id = -1;
    int32_t finished_num = 0;
    bool chains_write = IsChainsWrite();
    int32_t replica_num = write_windows_.size();
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
        finished_num = FinishedNum();
        while (!bg_error_) {
            if (finished_num == replica_num) {
                break;
            }
            // TODO flag for wait_time?
            if (!chains_write && finished_num == replica_num - 1 && wait_time >= 3) {
                LOG(WARNING, "Skip slow chunkserver");
                bg_error_ = true;
                break;
            }
            bool finish = sync_signal_.TimeWait(1000, (name_ + " Close wait").c_str());
            if (!finish && ++wait_time > 30 && (wait_time % 10 == 0)) {
                LOG(WARNING, "Close timeout %d s, %s back_writing_= %d, finish = %d",
                wait_time, name_.c_str(), back_writing_, finish);
            }
            finished_num = FinishedNum();
        }
    }
    delete block_for_write_;
    block_for_write_ = NULL;
    delete chunkserver_;
    chunkserver_ = NULL;
    LOG(DEBUG, "File %s closed", name_.c_str());
    closed_ = true;
    int32_t ret = OK;
    if (bg_error_ && !EnoughReplica()) {
        LOG(WARNING, "Close file %s fail", name_.c_str());
        ret = TIMEOUT;
    }
    if (need_report_finish) {
        FinishBlockRequest request;
        FinishBlockResponse response;
        request.set_sequence_id(0);
        request.set_file_name(name_);
        request.set_block_id(block_id);
        request.set_block_version(last_seq_);
        request.set_block_size(write_offset_);
        request.set_close_with_error(bg_error_);
        bool rpc_ret = fs_->nameserver_client_->SendRequest(&NameServer_Stub::FinishBlock,
                                                   &request, &response, 15, 1);
        if (!(rpc_ret && response.status() == kOK))  {
            LOG(WARNING, "Close file %s fail, finish report returns %d, status: %s",
                    name_.c_str(), rpc_ret, StatusCode_Name(response.status()).c_str());
            if (!rpc_ret) {
                return TIMEOUT;
            } else {
                return GetErrorCode(response.status());
            }
        }
    }
    return ret;
}

bool FileImpl::IsChainsWrite() {
    return w_options_.write_mode == kWriteChains;
}

bool FileImpl::EnoughReplica() {
    int32_t last_write_finish_num = FinishedNum();
    int32_t replica_num = write_windows_.size();
    bool is_chains = IsChainsWrite();
    return is_chains ? last_write_finish_num == 1 :
                       last_write_finish_num >= replica_num - 1;
}

} // namespace bfs
} // namespace baidu
