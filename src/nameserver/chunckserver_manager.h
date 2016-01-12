// Copyright (c) 2014, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "block_mapping.h"

#include <set>
#include <map>

#include <common/thread_pool.h>

namespace baidu {
namespace bfs {

class ChunkServerManager {
public:
    ChunkServerManager(ThreadPool* thread_pool, BlockMapping* block_manager);
    void DeadCheck();
    void IncChunkServerNum();
    int32_t GetChunkServerNum();
    void HandleRegister(const RegisterRequest* request, RegisterResponse* response);
    void HandleHeartBeat(const HeartBeatRequest* request, HeartBeatResponse* response);
    void ListChunkServers(::google::protobuf::RepeatedPtrField<ChunkServerInfo>* chunkservers);
    bool GetChunkServerChains(int num, std::vector<std::pair<int32_t,std::string> >* chains);
    int32_t AddChunkServer(const std::string& address, int64_t quota);
    bool UpdateChunkServer(int cs_id, int64_t quota);
    bool RemoveChunkServer(const std::string& address);
    std::string GetChunkServerAddr(int32_t id);
    int32_t GetChunkserverId(const std::string& address);
    void AddBlock(int32_t id, int64_t block_id);
    void RemoveBlock(int32_t id, int64_t block_id);
    void CleanChunkserver(ChunkServerInfo* cs, const std::string& reason);
    void PickRecoverBlocks(int64_t cs_id, int64_t block_num, std::map<int64_t, std::string>* recover_blocks);
private:
    ThreadPool* thread_pool_;
    BlockMapping* block_manager_;
    Mutex mu_;      /// chunkserver_s list mutext;
    typedef std::map<int32_t, ChunkServerInfo*> ServerMap;
    ServerMap chunkservers_;
    std::map<std::string, int32_t> address_map_;
    std::map<int32_t, std::set<ChunkServerInfo*> > heartbeat_list_;
    std::map<int32_t, std::set<int64_t> > chunkserver_block_map_;
    int32_t chunkserver_num_;
    int32_t next_chunkserver_id_;
};

} // namespace bfs
} // namespace baidu
