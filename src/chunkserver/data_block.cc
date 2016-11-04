// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#include "chunkserver/data_block.h"

#include <errno.h>
#include <fcntl.h>
#include <string.h>
#include <sys/stat.h>
#include <functional>

#include <gflags/gflags.h>
#include <common/counter.h>
#include <common/thread_pool.h>
#include <common/sliding_window.h>

#include <common/logging.h>

#include "file_cache.h"

DECLARE_int32(write_buf_size);

namespace baidu {
namespace bfs {

extern common::Counter g_block_buffers;
extern common::Counter g_buffers_new;
extern common::Counter g_buffers_delete;
extern common::Counter g_blocks;
extern common::Counter g_writing_blocks;
extern common::Counter g_pending_writes;
extern common::Counter g_writing_bytes;
extern common::Counter g_find_ops;
extern common::Counter g_read_ops;
extern common::Counter g_write_ops;
extern common::Counter g_write_bytes;
extern common::Counter g_refuse_ops;
extern common::Counter g_rpc_delay;
extern common::Counter g_rpc_delay_all;
extern common::Counter g_rpc_count;
extern common::Counter g_data_size;

Block::Block(const BlockMeta& meta, ThreadPool* thread_pool, FileCache* file_cache) :
  thread_pool_(thread_pool), meta_(meta),
  last_seq_(-1), slice_num_(-1), blockbuf_(NULL), buflen_(0),
  bufdatalen_(0), disk_writing_(false),
  disk_file_size_(meta.block_size()), file_desc_(kNotCreated), refs_(0),
  close_cv_(&mu_), is_recover_(false), deleted_(false),
  file_cache_(file_cache) {
    assert(meta_.block_id() < (1L<<40));
    g_data_size.Add(meta.block_size());
    disk_file_ = meta.store_path() + BuildFilePath(meta_.block_id());
    g_blocks.Inc();
    if (meta_.version() >= 0) {
        finished_ = true;
        recv_window_ = NULL;
    } else {
        finished_ = false;
        recv_window_ = new common::SlidingWindow<Buffer>(100,
                       std::bind(&Block::WriteCallback, this, std::placeholders::_1, std::placeholders::_2));
    }
}
Block::~Block() {
    if (bufdatalen_ > 0) {
        if (!deleted_) {
            LOG(WARNING, "Data lost, %d bytes in #%ld %s",
                bufdatalen_, meta_.block_id(), disk_file_.c_str());
        }
    }
    if (blockbuf_) {
        delete[] blockbuf_;
        g_block_buffers.Dec();
        g_buffers_delete.Inc();
        blockbuf_ = NULL;
    }
    buflen_ = 0;
    bufdatalen_ = 0;

    LOG(INFO, "Release #%ld block_buf_list_ size= %lu",
        meta_.block_id(), block_buf_list_.size());
    for (uint32_t i = 0; i < block_buf_list_.size(); i++) {
        const char* buf = block_buf_list_[i].first;
        int len = block_buf_list_[i].second;
        if (!deleted_) {
            LOG(WARNING, "Data lost, %d bytes in %s, #%ld block_buf_list_",
                len, disk_file_.c_str(), meta_.block_id());
        } else {
            LOG(INFO, "Release block_buf_list_ %d for #%ld ", len, meta_.block_id());
        }
        delete[] buf;
        g_block_buffers.Dec();
        g_pending_writes.Dec();
        g_buffers_delete.Inc();
    }
    block_buf_list_.clear();

    if (file_desc_ >= 0) {
        close(file_desc_);
        g_writing_blocks.Dec();
        file_desc_ = kClosed;
    }
    if (recv_window_) {
        if (recv_window_->Size()) {
            LOG(INFO, "#%ld recv_window fragments: %d\n",
                meta_.block_id(), recv_window_->Size());
            std::vector<std::pair<int32_t,Buffer> > frags;
            recv_window_->GetFragments(&frags);
            for (uint32_t i = 0; i < frags.size(); i++) {
                delete[] frags[i].second.data_;
            }
        }
        delete recv_window_;
    }
    LOG(INFO, "Block #%ld deconstruct", meta_.block_id());
    g_blocks.Dec();
    g_data_size.Sub(meta_.block_size());
}
/// Getter
int64_t Block::Id() const {
    return meta_.block_id();
}
int64_t Block::Size() const {
    return meta_.block_size();
}
std::string Block::GetFilePath() const {
    return disk_file_;
}
BlockMeta Block::GetMeta() const {
    return meta_;
}
int64_t Block::DiskUsed() {
    return disk_file_size_;
}
bool Block::SetDeleted() {
    int deleted = common::atomic_swap(&deleted_, 1);
    return (0 == deleted);
}
void Block::SetVersion(int64_t version) {
    meta_.set_version(std::max(version, meta_.version()));
}
int Block::GetVersion() {
    return meta_.version();
}
int32_t Block::GetLastSaq() {
    return last_seq_;
}

std::string Block::BuildFilePath(int64_t block_id) {
    char file_path[16];
    int len = snprintf(file_path, sizeof(file_path), "/%03ld/%010ld",
        block_id % 1000, block_id / 1000);
    assert (len == 15);
    return file_path;
}
/// Open corresponding file for write.
bool Block::OpenForWrite() {
    mu_.AssertHeld();
    if (file_desc_ >= 0) return true;
    /// Unlock for disk operating.
    mu_.Unlock();
    std::string dir = disk_file_.substr(0, disk_file_.rfind('/'));
    // Mkdir dir for data block, ignore error, may already exist.
    mkdir(dir.c_str(), 0755);
    int fd  = open(disk_file_.c_str(), O_WRONLY | O_CREAT | O_TRUNC, S_IRUSR);
    mu_.Lock("Block::OpenForWrite");
    if (fd < 0) {
        LOG(WARNING, "Open block #%ld %s fail: %s",
            meta_.block_id(), disk_file_.c_str(), strerror(errno));
        return false;
    }
    g_writing_blocks.Inc();
    file_desc_ = fd;
    return true;
}
/// Set expected slice num, for IsComplete.
void Block::SetSliceNum(int32_t num) {
    slice_num_ = num;
}
/// Is all slice is arrival(Notify by the sliding window)
bool Block::IsComplete() {
    return (slice_num_ == last_seq_ + 1);
}
bool Block::IsFinished() {
    return finished_;
}
/// Read operation.
int64_t Block::Read(char* buf, int64_t len, int64_t offset) {
    MutexLock lock(&mu_, "Block::Read", 1000);
    if (offset > meta_.block_size()) {
        LOG(INFO, "Wrong offset %ld > %ld", offset, meta_.block_size());
        return -1;
    } else if (deleted_) {
        return -1;
    }

    /// Read from disk
    int64_t readlen = 0;
    while (offset + readlen < disk_file_size_) {
        int64_t pread_len = std::min(len - readlen, disk_file_size_ - offset - readlen);
        mu_.Unlock();
        int64_t ret = file_cache_->ReadFile(disk_file_,
                        buf + readlen, pread_len, offset + readlen);
        mu_.Lock("Block::Read relock", 1000);
        if (ret != pread_len) {
            LOG(WARNING, "ReadFile fail: pread_len: %ld offset: %ld ret: %ld %s",
                    pread_len, offset + readlen, ret, strerror(errno));
            return -2;
        }
        readlen += ret;
        if (readlen >= len) return readlen;
        // If disk_file_size change, read again.
    }
    // Read from block_buf_list
    int64_t mem_offset = offset + readlen - disk_file_size_;
    uint32_t buf_id = mem_offset / FLAGS_write_buf_size;
    mem_offset %= FLAGS_write_buf_size;
    while (buf_id < block_buf_list_.size()) {
        const char* block_buf = block_buf_list_[buf_id].first;
        int buf_len = block_buf_list_[buf_id].second;
        int mlen = std::min(len - readlen, buf_len - mem_offset);
        memcpy(buf + readlen, block_buf + mem_offset, mlen);
        readlen += mlen;
        mem_offset = 0;
        buf_id ++;
        if (readlen >= len) return readlen;
    }
    // Read from block buf
    assert (mem_offset >= 0);
    if (mem_offset < bufdatalen_) {
        int mlen = std::min(bufdatalen_ - mem_offset, len - readlen);
        memcpy(buf + readlen, blockbuf_ + mem_offset, mlen);
        readlen += mlen;
    }

    return readlen;
}
/// Write operation.
bool Block::Write(int32_t seq, int64_t offset, const char* data,
                  int64_t len, int64_t* add_use) {
    MutexLock lock(&mu_, "BlockWrite", 1000);
    if (finished_ || deleted_) {
        LOG(INFO, "Write a finish block #%ld V%ld %ld, seq: %d, offset: %ld, finished: %d, deleted: %d",
            meta_.block_id(), meta_.version(), meta_.block_size(), seq, offset, finished_, deleted_);
        return false;
    }
    if (offset < meta_.block_size()) {
        LOG(INFO, "Write #%ld size %ld, seq: %d, wrong offset: %ld",
            meta_.block_id(), meta_.block_size(), seq, offset);
        assert (offset + len <= meta_.block_size());
        return true;
    }
    char* buf = NULL;
    if (len) {
        buf = new char[len];
        memcpy(buf, data, len);
        g_writing_bytes.Add(len);
    }
    int64_t add_start = common::timer::get_micros();
    int ret = recv_window_->Add(seq, Buffer(buf, len));
    if (add_use) *add_use = common::timer::get_micros() - add_start;
    if (ret != 0) {
        delete[] buf;
        g_writing_bytes.Sub(len);
        if (ret < 0) {
            LOG(WARNING, "Write block #%ld seq: %d, offset: %ld, block_size: %ld"
                         " out of range %d",
                meta_.block_id(), seq, offset, meta_.block_size(), recv_window_->UpBound());
            return false;
        }
    }
    if (ret == 0) {
        g_write_bytes.Add(len);
    }
    return true;
}
/// Flush block to disk.
bool Block::Close() {
    MutexLock lock(&mu_, "Block::Close", 1000);
    if (finished_) {
        return false;
    }

    block_buf_list_.push_back(std::make_pair(blockbuf_, bufdatalen_));
    g_pending_writes.Inc();
    blockbuf_ = NULL;
    bufdatalen_ = 0;

    finished_ = true;
    // DiskWrite will close file_desc_ asynchronously.
    this->AddRef();
    thread_pool_->AddPriorityTask(std::bind(&Block::DiskWrite, this));

    while (file_desc_ != kClosed) {
        close_cv_.Wait();
    }
    if (meta_.version() == -1) {
        SetVersion(last_seq_);
    }
    LOG(INFO, "Block #%ld closed %s V%ld %ld",
        meta_.block_id(), disk_file_.c_str(), meta_.version(), meta_.block_size());
    return true;
}

void Block::AddRef() {
    common::atomic_inc(&refs_);
    assert (refs_ > 0);
}
void Block::DecRef() {
    if (common::atomic_add(&refs_, -1) == 1) {
        assert(refs_ == 0);
        delete this;
    }
}
int Block::GetRef() {
    return refs_;
}
/// Invoke by slidingwindow, when next buffer arrive.
void Block::WriteCallback(int32_t seq, Buffer buffer) {
    Append(seq, buffer.data_, buffer.len_);
    delete[] buffer.data_;
    g_writing_bytes.Sub(buffer.len_);
}
void Block::DiskWrite() {
    {
        MutexLock lock(&mu_, "Block::DiskWrite", 1000);
        if (disk_writing_) {
            this->DecRef();
            return;
        }
        if (!deleted_) {
            disk_writing_ = true;
            while (!block_buf_list_.empty() && !deleted_) {
                if (!OpenForWrite()) assert(0);
                const char* buf = block_buf_list_[0].first;
                int len = block_buf_list_[0].second;

                // Unlock when disk write
                mu_.Unlock();
                int wlen = 0;
                while (wlen < len) {
                    int w = write(file_desc_, buf + wlen, len - wlen);
                    if (w < 0) {
                        LOG(WARNING, "IOError write #%ld %s return %s",
                            meta_.block_id(), disk_file_.c_str(), strerror(errno));
                        assert(0);
                        break;
                    }
                    wlen += w;
                }
                // Re-Lock for commit
                mu_.Lock("Block::DiskWrite ReLock", 1000);
                block_buf_list_.erase(block_buf_list_.begin());
                delete[] buf;
                g_pending_writes.Dec();
                g_block_buffers.Dec();
                g_buffers_delete.Inc();
                disk_file_size_ += len;
            }
            disk_writing_ = false;
        }
        if (finished_ || deleted_) {
            assert (deleted_ || block_buf_list_.empty());
            if (file_desc_ >= 0) {
                int ret = close(file_desc_);
                LOG(INFO, "[DiskWrite] close file %s", disk_file_.c_str());
                assert(ret == 0);
                g_writing_blocks.Dec();
            }
            file_desc_ = kClosed;
            //free sliding window when fd is closed
            if (recv_window_ && recv_window_->Size()) {
                LOG(INFO, "#%ld recv_window fragments: %d\n",
                        meta_.block_id(), recv_window_->Size());
                std::vector<std::pair<int32_t,Buffer> > frags;
                recv_window_->GetFragments(&frags);
                for (uint32_t i = 0; i < frags.size(); i++) {
                    delete[] frags[i].second.data_;
                    g_writing_bytes.Sub(frags[i].second.len_);
                }
                delete recv_window_;
                recv_window_ = NULL;
            }
            close_cv_.Signal();
        }
    }
    this->DecRef();
}
void Block::SetRecover() {
    is_recover_ = true;
}
bool Block::IsRecover() {
    return is_recover_;
}
/// Append to block buffer
StatusCode Block::Append(int32_t seq, const char* buf, int64_t len) {
    mu_.AssertHeld();
    if (blockbuf_ == NULL) {
        buflen_ = FLAGS_write_buf_size;
        blockbuf_ = new char[buflen_];
        g_block_buffers.Inc();
        g_buffers_new.Inc();
    }
    int64_t ap_len = len;
    while (bufdatalen_ + ap_len > buflen_) {
        int64_t wlen = buflen_ - bufdatalen_;
        memcpy(blockbuf_ + bufdatalen_, buf, wlen);
        block_buf_list_.push_back(std::make_pair(blockbuf_, FLAGS_write_buf_size));
        this->AddRef();
        thread_pool_->AddTask(std::bind(&Block::DiskWrite, this));

        blockbuf_ = new char[buflen_];
        g_pending_writes.Inc();
        g_block_buffers.Inc();
        g_buffers_new.Inc();
        bufdatalen_ = 0;
        buf += wlen;
        ap_len -= wlen;
    }
    if (ap_len) {
        memcpy(blockbuf_ + bufdatalen_, buf, ap_len);
        bufdatalen_ += ap_len;
    }
    meta_.set_block_size(meta_.block_size() + len);
    g_data_size.Add(len);
    last_seq_ = seq;
    return kOK;
}

} // namespace bfs
} // namespace baidu

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
