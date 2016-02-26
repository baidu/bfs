// Copyright (c) 2014, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <set>
#include <map>

#include <common/thread_pool.h>
#include "proto/nameserver.pb.h"

namespace baidu {
namespace bfs {

class BlockMapping;

class ChunkServerManager {
public:
    struct Stats {
        int32_t w_qps;
        int64_t w_speed;
        int32_t r_qps;
        int64_t r_speed;
        int64_t recover_speed;
    };
    ChunkServerManager(ThreadPool* thread_pool, BlockMapping* block_mapping);
    void HandleRegister(const RegisterRequest* request, RegisterResponse* response);
    void HandleHeartBeat(const HeartBeatRequest* request, HeartBeatResponse* response);
    void ListChunkServers(::google::protobuf::RepeatedPtrField<ChunkServerInfo>* chunkservers);
    bool GetChunkServerChains(int num, std::vector<std::pair<int32_t,std::string> >* chains,
                              const std::string& client_address);
    bool GetRecoverChains(const std::set<int32_t>& replica, std::vector<std::string>* chains);
    int32_t AddChunkServer(const std::string& address, int64_t quota);
    bool KickChunkserver(int cs_id);
    bool UpdateChunkServer(int cs_id, int64_t quota);
    bool RemoveChunkServer(const std::string& address);
    std::string GetChunkServerAddr(int32_t id);
    int32_t GetChunkserverId(const std::string& address);
    void AddBlock(int32_t id, int64_t block_id);
    void RemoveBlock(int32_t id, int64_t block_id);
    void CleanChunkserver(ChunkServerInfo* cs, const std::string& reason);
    void PickRecoverBlocks(int cs_id, std::map<int64_t, std::vector<std::string> >* recover_blocks,
                           int* hi_num);
    void GetStat(int32_t* w_qps, int64_t* w_speed, int32_t* r_qps,
                 int64_t* r_speed, int64_t* recover_speed);
private:
    void DeadCheck();
    int GetChunkserverLoad(ChunkServerInfo* cs);
    void RandomSelect(std::vector<std::pair<int, ChunkServerInfo*> >* loads, int num);
    bool GetChunkServerPtr(int32_t cs_id, ChunkServerInfo** cs);
    void LogStats();
private:
    ThreadPool* thread_pool_;
    BlockMapping* block_mapping_;
    Mutex mu_;      /// chunkserver_s list mutext;
    Stats stats_;
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
