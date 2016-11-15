// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#ifndef  BAIDU_BFS_DATA_BLOCK_H_
#define  BAIDU_BFS_DATA_BLOCK_H_

#include <string>
#include <vector>

#include <common/mutex.h>
#include <common/thread_pool.h>
#include <common/sliding_window.h>
#include "proto/status_code.pb.h"

#include "proto/block.pb.h"

namespace baidu {
namespace bfs {

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


class FileCache;

/// Data block
class Block {
public:
    Block(const BlockMeta& meta, ThreadPool* thread_pool, FileCache* file_cache);
    ~Block();
    static std::string BuildFilePath(int64_t block_id);
    /// Getter
    int64_t Id() const;
    int64_t Size() const;
    std::string GetFilePath() const;
    BlockMeta GetMeta() const;
    int64_t DiskUsed();
    bool SetDeleted();
    void SetVersion(int64_t version);
    int GetVersion();
    int32_t GetLastSaq();
    /// Set expected slice num, for IsComplete.
    void SetSliceNum(int32_t num);
    /// Is all slice is arrival(Notify by the sliding window)
    bool IsComplete();
    /// Block is closed
    bool IsFinished();
    /// Read operation.
    int64_t Read(char* buf, int64_t len, int64_t offset);
    /// Write operation.
    bool Write(int32_t seq, int64_t offset, const char* data,
               int64_t len, int64_t* add_use = NULL);
    /// Append to block buffer
    StatusCode Append(int32_t seq, const char*buf, int64_t len);
    void SetRecover();
    bool IsRecover();
    /// Flush block to disk.
    bool Close();
    void AddRef();
    void DecRef();
    int GetRef();
private:
    /// Open corresponding file for write.
    bool OpenForWrite();
    /// Invoke by slidingwindow, when next buffer arrive.
    void WriteCallback(int32_t seq, Buffer buffer);
    void DiskWrite();
private:
    enum Type {
        InDisk,
        InMem,
    };
    enum FdStatus {
        kNotCreated = -1,
        kClosed = -2
    };
    ThreadPool* thread_pool_;
    BlockMeta   meta_;
    int32_t     last_seq_;
    int32_t     slice_num_;
    char*       blockbuf_;
    int64_t     buflen_;
    int64_t     bufdatalen_;
    std::vector<std::pair<const char*,int> > block_buf_list_;
    bool        disk_writing_;
    std::string disk_file_;
    int64_t     disk_file_size_;
    int         file_desc_; ///< disk file fd
    volatile int refs_;
    Mutex       mu_;
    CondVar     close_cv_;
    common::SlidingWindow<Buffer>* recv_window_;
    bool        is_recover_;
    bool        finished_;
    volatile int deleted_;

    FileCache*  file_cache_;
};

}
}

#endif  // BAIDU_BFS_BLOCK_H_

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
