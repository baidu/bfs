// Copyright (c) 2014, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <set>
#include <map>
#include <functional>

#include <common/thread_pool.h>
#include "proto/nameserver.pb.h"
#include "proto/status_code.pb.h"

namespace baidu {
namespace bfs {

const double kChunkServerLoadMax = 0.999999;

class BlockMappingManager;
typedef  std::vector<std::pair<int64_t, std::vector<std::string> > > RecoverVec;

class Blocks {
public:
    Blocks(int32_t cs_id) : report_id_(-1), cs_id_(cs_id) {}
    int64_t GetReportId();
    void Insert(int64_t block_id);
    void Remove(int64_t block_id);
    void CleanUp(std::set<int64_t>* blocks);
    int64_t CheckLost(int64_t report_id, std::set<int64_t>& blocks,
                      int64_t start, int64_t end, std::vector<int64_t>* lost);
private:
    Mutex block_mu_;
    std::set<int64_t> blocks_;
    Mutex new_blocks_mu_;
    std::set<int64_t> new_blocks_;
    int64_t report_id_;
    int32_t cs_id_;                 // for debug msg
};

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
    bool HandleRegister(const std::string& ip,
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
    bool UpdateChunkServer(int cs_id, const std::string& tag, int64_t quota);
    bool RemoveChunkServer(const std::string& address);
    std::string GetChunkServerAddr(int32_t id);
    int32_t GetChunkServerId(const std::string& address);
    void AddBlock(int32_t id, int64_t block_id);
    void RemoveBlock(int32_t id, int64_t block_id);
    void CleanChunkServer(ChunkServerInfo* cs, const std::string& reason);
    void PickRecoverBlocks(int cs_id,  RecoverVec* recover_blocks, int* hi_num, bool hi_only);
    void GetStat(int32_t* w_qps, int64_t* w_speed, int32_t* r_qps,
                 int64_t* r_speed, int64_t* recover_speed);
    StatusCode ShutdownChunkServer(const::google::protobuf::RepeatedPtrField<std::string>& chunkserver_address);
    bool GetShutdownChunkServerStat();
    int64_t AddBlockWithCheck(int32_t id, std::set<int64_t>& blocks, int64_t start, int64_t end,
                  std::vector<int64_t>* lost, int64_t report_id);
    void SetParam(const Params& p);
private:
    double GetChunkServerLoad(ChunkServerInfo* cs);
    void DeadCheck();
    void RandomSelect(std::vector<std::pair<double, ChunkServerInfo*> >* loads, int num);
    bool GetChunkServerPtr(int32_t cs_id, ChunkServerInfo** cs);
    void LogStats();
    int SelectChunkServerByZone(int num,
        const std::vector<std::pair<double, ChunkServerInfo*> >& loads,
        std::vector<std::pair<int32_t,std::string> >* chains);
    void MarkChunkServerReadonly(const std::string& chunkserver_address);
    Blocks* GetBlockMap(int32_t cs_id);
private:
    ThreadPool* thread_pool_;
    BlockMappingManager* block_mapping_manager_;
    Mutex mu_;      /// chunkserver_s list mutext;
    Stats stats_;
    typedef std::map<int32_t, ChunkServerInfo*> ServerMap;
    ServerMap chunkservers_;
    std::map<std::string, int32_t> address_map_;
    std::map<int32_t, std::set<ChunkServerInfo*> > heartbeat_list_;
    std::map<int32_t, Blocks*> block_map_;
    int32_t chunkserver_num_;
    int32_t next_chunkserver_id_;

    std::string localhostname_;
    std::string localzone_;

    std::vector<std::string> chunkservers_to_offline_;

    // for chunkserver
    Params params_;
};


} // namespace bfs
} // namespace baidu
