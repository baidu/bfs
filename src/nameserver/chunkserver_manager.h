// Copyright (c) 2014, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <set>
#include <map>

#include <common/thread_pool.h>
#include "proto/nameserver.pb.h"

namespace baidu {
namespace bfs {

const double kChunkServerLoadMax = 0.999999;

class BlockMappingManager;

class ChunkServerManager {
public:
    struct Stats {
        int32_t w_qps;
        int64_t w_speed;
        int32_t r_qps;
        int64_t r_speed;
        int64_t recover_speed;
    };
    ChunkServerManager(ThreadPool* thread_pool, BlockMappingManager* block_mapping_manager);
    void HandleRegister(const std::string& ip,
                        const RegisterRequest* request,
                        RegisterResponse* response);
    void HandleHeartBeat(const HeartBeatRequest* request, HeartBeatResponse* response);
    void ListChunkServers(::google::protobuf::RepeatedPtrField<ChunkServerInfo>* chunkservers);
    bool GetChunkServerChains(int num, std::vector<std::pair<int32_t,std::string> >* chains,
                              const std::string& client_address);
    bool GetRecoverChains(const std::set<int32_t>& replica, std::vector<std::string>* chains);
    int32_t AddChunkServer(const std::string& address, const std::string& ip,
                           const std::string& tag, int64_t quota);
    bool KickChunkServer(int cs_id);
    bool UpdateChunkServer(int cs_id, int64_t quota);
    bool RemoveChunkServer(const std::string& address);
    std::string GetChunkServerAddr(int32_t id);
    int32_t GetChunkServerId(const std::string& address);
    void AddBlock(int32_t id, int64_t block_id);
    void RemoveBlock(int32_t id, int64_t block_id);
    void CleanChunkServer(ChunkServerInfo* cs, const std::string& reason);
    void PickRecoverBlocks(int cs_id,
                           std::vector<std::pair<int64_t, std::vector<std::string> > >* recover_blocks,
                           int* hi_num);
    void GetStat(int32_t* w_qps, int64_t* w_speed, int32_t* r_qps,
                 int64_t* r_speed, int64_t* recover_speed);
    StatusCode ShutdownChunkServer(const::google::protobuf::RepeatedPtrField<std::string>& chunkserver_address);
    bool GetShutdownChunkServerStat();
    void AddBlock(int32_t id, const::google::protobuf::RepeatedPtrField<ReportBlockInfo>& blocks);
private:
    struct ChunkServerBlockMap {
        Mutex* mu;
        std::set<int64_t> blocks;
        ChunkServerBlockMap() {
            mu = new Mutex;
        }
        ~ChunkServerBlockMap() {
            delete mu;
        }
    };
    double GetChunkServerLoad(ChunkServerInfo* cs);
    void DeadCheck();
    void RandomSelect(std::vector<std::pair<double, ChunkServerInfo*> >* loads, int num);
    bool GetChunkServerPtr(int32_t cs_id, ChunkServerInfo** cs);
    void LogStats();
    int SelectChunkServerByZone(int num,
        const std::vector<std::pair<double, ChunkServerInfo*> >& loads,
        std::vector<std::pair<int32_t,std::string> >* chains);
    void MarkChunkServerReadonly(const std::string& chunkserver_address);
    bool GetChunkServerBlockMapPtr(int32_t cs_id, ChunkServerBlockMap** cs_block_map);
private:
    ThreadPool* thread_pool_;
    BlockMappingManager* block_mapping_manager_;
    Mutex mu_;      /// chunkserver_s list mutext;
    Stats stats_;
    typedef std::map<int32_t, ChunkServerInfo*> ServerMap;
    ServerMap chunkservers_;
    std::map<std::string, int32_t> address_map_;
    std::map<int32_t, std::set<ChunkServerInfo*> > heartbeat_list_;
    std::map<int32_t, ChunkServerBlockMap*> chunkserver_block_map_;
    int32_t chunkserver_num_;
    int32_t next_chunkserver_id_;

    std::string localhostname_;
    std::string localzone_;

    std::vector<std::string> chunkservers_to_offline_;
};


} // namespace bfs
} // namespace baidu
