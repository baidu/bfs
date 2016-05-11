#ifndef CLIENT_MANAGER_H_
#define CLIENT_MANAGER_H_

#include <string>
#include <set>

#include <common/thread_pool.h>
#include <common/mutex.h>

#include "proto/status_code.pb.h"

namespace baidu {
namespace bfs {

class BlockMapping;

class ClientManager {
public:
    struct ClientInfo {
        int32_t last_heartbeat_time;
        std::set<int64_t> writing_blocks;
        std::string session_id;
    };
    ClientManager(BlockMapping* block_mapping);
    ~ClientManager();
    StatusCode AddNewClient(const std::string& session_id);
    StatusCode HandleHeartbeat(const std::string& session_id);
    void AddWritingBlock(const std::string& session_id, int64_t block_id);
    void RemoveWritingBlock(const std::string& session_id, int64_t block_id);
private:
    void DeadCheck();
    std::map<std::string, ClientInfo*> client_map_;
    std::map<int32_t, std::set<ClientInfo*> > heartbeat_list_;
    ThreadPool* thread_pool_;
    BlockMapping* block_mapping_;
    Mutex mu_;
};

}
}
#endif
