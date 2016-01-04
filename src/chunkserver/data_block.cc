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

#include <boost/bind.hpp>
#include <gflags/gflags.h>
#include "common/counter.h"
#include "common/thread_pool.h"
#include "common/sliding_window.h"

#include "common/logging.h"

#include "file_cache.h"

DECLARE_int32(write_buf_size);

namespace baidu {
namespace bfs {

extern common::Counter g_block_buffers;
extern common::Counter g_buffers_new;
extern common::Counter g_buffers_delete;
extern common::Counter g_blocks;
extern common::Counter g_writing_blocks;
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

Block::Block(const BlockMeta& meta, const std::string& store_path, ThreadPool* thread_pool,
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
Block::~Block() {
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
        const char* buf = _block_buf_list[i].first;
        int len = _block_buf_list[i].second;
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
        g_writing_blocks.Dec();
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
int64_t Block::Id() const {
    return _meta.block_id;
}
int64_t Block::Size() const {
    return _meta.block_size;
}
std::string Block::GetFilePath() const {
    return _disk_file;
}
BlockMeta Block::GetMeta() const {
    return _meta;
}
int64_t Block::DiskUsed() {
    return _disk_file_size;
}
bool Block::SetDeleted() {
    int deleted = common::atomic_swap(&_deleted, 1);
    return (0 == deleted);
}
void Block::SetVersion(int64_t version) {
    _meta.version = std::max(version, _meta.version);
}
int Block::GetVersion() {
    return _meta.version;
}
/// Open corresponding file for write.
bool Block::OpenForWrite() {
    _mu.AssertHeld();
    if (_file_desc >= 0) return true;
    /// Unlock for disk operating.
    _mu.Unlock();
    std::string dir = _disk_file.substr(0, _disk_file.rfind('/'));
    // Mkdir dir for data block, ignore error, may already exist.
    mkdir(dir.c_str(), 0755);
    int fd  = open(_disk_file.c_str(), O_WRONLY | O_CREAT | O_TRUNC, S_IRUSR);
    if (fd < 0) {
        LOG(WARNING, "Open block #%ld %s fail: %s",
            _meta.block_id, _disk_file.c_str(), strerror(errno));
        return false;
    }
    _mu.Lock("Block::OpenForWrite");
    g_writing_blocks.Inc();
    _file_desc = fd;
    return true;
}
/// Set expected slice num, for IsComplete.
void Block::SetSliceNum(int32_t num) {
    _slice_num = num;
}
/// Is all slice is arrival(Notify by the sliding window)
bool Block::IsComplete() {
    return (_slice_num == _last_seq + 1);
}
/// Read operation.
int32_t Block::Read(char* buf, int32_t len, int64_t offset) {
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
        if (ret != pread_len) {
            return -2;
        }
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
bool Block::Write(int32_t seq, int64_t offset, const char* data,
           int32_t len, int64_t* add_use) {
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
                         " out of range %d",
                _meta.block_id, seq, offset, _meta.block_size, _recv_window->UpBound());
            return false;
        }
    }
    if (ret >= 0) {
        g_write_bytes.Add(len);
    }
    return true;
}
/// Flush block to disk.
bool Block::Close() {
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
void Block::AddRef() {
    common::atomic_inc(&_refs);
    assert (_refs > 0);
}
void Block::DecRef() {
    if (common::atomic_add(&_refs, -1) == 1) {
        assert(_refs == 0);
        delete this;
    }
}

/// Invoke by slidingwindow, when next buffer arrive.
void Block::WriteCallback(int32_t seq, Buffer buffer) {
    Append(seq, buffer.data_, buffer.len_);
    delete[] buffer.data_;
    g_writing_bytes.Sub(buffer.len_);
}
void Block::DiskWrite() {
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
            // Re-Lock for commit
            _mu.Lock("Block::DiskWrite ReLock", 1000);
            _block_buf_list.erase(_block_buf_list.begin());
            delete[] buf;
            g_block_buffers.Dec();
            g_buffers_delete.Inc();
            _disk_file_size += len;
        }
        _disk_writing = false;
    }
    if (!_disk_writing && (_finished || _deleted)) {
        if (_file_desc != -1) {
            int ret = close(_file_desc);
            LOG(INFO, "[DiskWrite] close file %s", _disk_file.c_str());
            assert(ret == 0);
            g_writing_blocks.Dec();
        }
        _file_desc = -1;
    }
    this->DecRef();
}
/// Append to block buffer
void Block::Append(int32_t seq, const char*buf, int32_t len) {
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

} // namespace bfs
} // namespace baidu

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
