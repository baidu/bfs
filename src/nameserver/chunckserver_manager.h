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
    void HandleHeartBeat(const HeartBeatRequest* request, HeartBeatResponse* response);
    void ListChunkServers(::google::protobuf::RepeatedPtrField<ChunkServerInfo>* chunkservers);
    bool GetChunkServerChains(int num, std::vector<std::pair<int32_t,std::string> >* chains);
    int64_t AddChunkServer(const std::string& address, int64_t quota, int cs_id = -1);
    std::string GetChunkServerAddr(int32_t id);
    int32_t GetChunkserverId(const std::string& addr);
    void AddBlock(int32_t id, int64_t block_id);
    void RemoveBlock(int32_t id, int64_t block_id);

private:
    ThreadPool* _thread_pool;
    BlockMapping* _block_manager;
    Mutex _mu;      /// _chunkservers list mutext;
    typedef std::map<int32_t, ChunkServerInfo*> ServerMap;
    ServerMap _chunkservers;
    std::map<std::string, int32_t> _address_map;
    std::map<int32_t, std::set<ChunkServerInfo*> > _heartbeat_list;
    std::map<int32_t, std::set<int64_t> > _chunkserver_block_map;
    int32_t _chunkserver_num;
    int32_t _next_chunkserver_id;
};

} // namespace bfs
} // namespace baidu
