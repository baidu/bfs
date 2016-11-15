// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//

#ifndef  BFS_SDK_FILE_IMPL_H_
#define  BFS_SDK_FILE_IMPL_H_

#include <map>
#include <set>
#include <string>
#include <memory>

#include <common/atomic.h>
#include <common/mutex.h>
#include <common/timer.h>
#include <common/sliding_window.h>
#include <common/thread_pool.h>

#include "proto/nameserver.pb.h"
#include "proto/chunkserver.pb.h"

#include "bfs.h"

namespace baidu {
namespace bfs {

class WriteBuffer {
public:
    WriteBuffer(int32_t seq, int32_t buf_size, int64_t block_id, int64_t offset);
    ~WriteBuffer();
    int Available();
    int Append(const char* buf, int len);
    const char* Data();
    int Size() const;
    int Sequence() const;
    void Clear();
    void SetLast();
    bool IsLast() const;
    int64_t offset() const;
    int64_t block_id() const;
    void AddRefBy(int counter);
    void AddRef();
    void DecRef();
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

class FSImpl;
class RpcClient;

struct LocatedBlocks {
    int64_t file_length_;
    std::vector<LocatedBlock> blocks_;
    void CopyFrom(const ::google::protobuf::RepeatedPtrField<LocatedBlock>& blocks) {
        for (int i = 0; i < blocks.size(); i++) {
            blocks_.push_back(blocks.Get(i));
        }
    }
};

class FileImpl : public File, public std::enable_shared_from_this<FileImpl> {
public:
    FileImpl(FSImpl* fs, RpcClient* rpc_client, const std::string& name,
             int32_t flags, const WriteOptions& options);
    FileImpl(FSImpl* fs, RpcClient* rpc_client, const std::string& name,
             int32_t flags, const ReadOptions& options);
    ~FileImpl ();
    int32_t Pread(char* buf, int32_t read_size, int64_t offset, bool reada = false);
    int64_t Seek(int64_t offset, int32_t whence);
    int32_t Read(char* buf, int32_t read_size);
    int32_t Write(const char* buf, int32_t write_size);
    /// Add buffer to  async write list
    void StartWrite();
    /// Send local buffer to chunkserver
    static void BackgroundWrite(std::weak_ptr<FileImpl> wk_fp);
    /// Callback for sliding window
    static void OnWriteCommit(int32_t, int32_t);
    static void WriteBlockCallback(std::weak_ptr<FileImpl> wk_fp,
                            const WriteBlockRequest* request,
                            WriteBlockResponse* response,
                            bool failed, int error,
                            int retry_times,
                            WriteBuffer* buffer,
                            std::string cs_addr);
    /// When rpc buffer full deley send write reqeust
    static void DelayWriteChunk(std::weak_ptr<FileImpl> wk_fp,
                                WriteBuffer* buffer, const WriteBlockRequest* request,
                                int retry_times, std::string cs_addr);
    int32_t Flush();
    int32_t Sync();
    int32_t Close();

    struct WriteBufferCmp {
        bool operator()(const WriteBuffer* a, const WriteBuffer* b) {
            return a->Sequence() > b->Sequence();
        }
    };
    friend class FSImpl;
private:
    int32_t AddBlock();
    bool CheckWriteWindows();
    int32_t FinishedNum();
    bool ShouldSetError();
    void BackgroundWriteInternal();
    void WriteBlockCallbackInternal(const WriteBlockRequest* request,
                            WriteBlockResponse* response,
                            bool failed, int error,
                            int retry_times,
                            WriteBuffer* buffer,
                            std::string cs_addr);
    void DelayWriteChunkInternal(WriteBuffer* buffer, const WriteBlockRequest* request,
                                int retry_times, std::string cs_addr);
    bool IsChainsWrite();
    bool EnoughReplica();
private:
    FSImpl* fs_;                        ///< 文件系统
    RpcClient* rpc_client_;             ///< RpcClient
    ThreadPool* thread_pool_;           ///< ThreadPool
    std::string name_;                  ///< 文件路径
    int32_t open_flags_;                ///< 打开使用的flag

    /// for write
    volatile int64_t write_offset_;     ///< user write offset
    LocatedBlock* block_for_write_;     ///< 正在写的block
    WriteBuffer* write_buf_;            ///< 本地写缓冲
    int32_t last_seq_;                  ///< last sequence number
    std::map<std::string, common::SlidingWindow<int>* > write_windows_;
    std::priority_queue<WriteBuffer*, std::vector<WriteBuffer*>, WriteBufferCmp>
        write_queue_;                   ///< Write buffer list
    volatile int back_writing_;         ///< Async write running backgroud
    const WriteOptions w_options_;

    /// for read
    LocatedBlocks located_blocks_;      ///< block meta for read
    ChunkServer_Stub* chunkserver_;     ///< located chunkserver
    std::map<std::string, ChunkServer_Stub*> chunkservers_; ///< located chunkservers
    std::set<ChunkServer_Stub*> bad_chunkservers_;
    int32_t last_chunkserver_index_;
    int64_t read_offset_;               ///< 读取的偏移
    Mutex read_offset_mu_;
    char* reada_buffer_;                ///< Read ahead buffer
    int32_t reada_buf_len_;             ///< Read ahead buffer length
    int64_t reada_base_;                ///< Read ahead base offset
    int32_t sequential_ratio_;          ///< Sequential read ratio
    int64_t last_read_offset_;
    const ReadOptions r_options_;

    bool closed_;                       ///< 是否关闭
    bool synced_;                     ///< 是否调用过sync
    Mutex   mu_;
    CondVar sync_signal_;               ///< _sync_var
    bool bg_error_;                     ///< background write error
    std::map<std::string, bool> cs_errors_;        ///< background write error for each chunkserver
};

} // namespace bfs
} // namespace baidu

#endif  //BFS_SDK_FILE_IMPL_H_
