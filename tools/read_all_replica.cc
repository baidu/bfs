#include <stdio.h>
#include <string>
#include <map>
#include <set>
#include <boost/bind.hpp>

#include <common/mutex.h>
#include <common/timer.h>
#include <common/thread_pool.h>
#include <common/string_util.h>

#include "rpc/rpc_client.h"
#include "sdk/bfs.h"
#include "proto/nameserver.pb.h"
#include "proto/chunkserver.pb.h"


char* nameserver_address = NULL;
baidu::bfs::FS* fs = NULL;
baidu::common::ThreadPool* thread_pool = new baidu::common::ThreadPool(5);

void read_replica_of_block(int64_t block_id, std::string cs_addr) {
    std::string file_name = cs_addr + ":" + baidu::common::NumToString(block_id);
    baidu::bfs::RpcClient* client = new baidu::bfs::RpcClient();
    baidu::bfs::ChunkServer_Stub* chunkserver = NULL;
    if (!client->GetStub(cs_addr, &chunkserver)) {
        fprintf(stderr, "Get stub for %s fail\n", cs_addr.c_str());
        return;
    }
    FILE* fp = fopen(file_name.c_str(), "wb");
    assert(fp);
    int64_t offset = 0;
    while (true) {
        baidu::bfs::ReadBlockRequest request;
        baidu::bfs::ReadBlockResponse response;
        request.set_sequence_id(baidu::common::timer::get_micros());
        request.set_block_id(block_id);
        request.set_offset(offset);
        request.set_read_len(1024 * 1024);
        if (!client->SendRequest(chunkserver, &baidu::bfs::ChunkServer_Stub::ReadBlock, &request, &response, 15, 3)) {
            fprintf(stderr, "Read block #%ld of %s fail\n", block_id, cs_addr.c_str());
            return;
        }
        if (response.databuf().size() <= 0) {
            break;
        }
        fwrite(response.databuf().data(), response.databuf().size(), 1, fp);
        offset += response.databuf().size();
    }
    fclose(fp);
}

int main(int argc, char* argv[])
{
    if (argc != 3) {
        printf("Usage: <nameserver address> <file name>\n");
        return -1;
    }
    char* nameserver_address = argv[1];
    if (!baidu::bfs::FS::OpenFileSystem(nameserver_address, &fs)) {
        fprintf(stderr, "Open file system %s fail\n", nameserver_address);
        return -1;
    }
    char* file_name = argv[2];
    std::map<int64_t, std::vector<std::string> > locations;
    if (!fs->GetFileLocation(file_name, &locations)) {
        fprintf(stderr, "Get file %s location error\n");
        return -1;
    }
    for (std::map<int64_t, std::vector<std::string> >::iterator it = locations.begin();
            it != locations.end(); ++it) {
        int64_t block_id = it->first;
        for (size_t i = 0; i < it->second.size(); i++) {
            std::string cs_addr = (it->second)[i];
            thread_pool->AddTask(boost::bind(&read_replica_of_block, block_id, cs_addr));
        }
    }
    thread_pool->Stop(true);
}
