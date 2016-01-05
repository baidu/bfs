// Copyright (c) 2014, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#include "chunkserver_impl.h"

#include <errno.h>
#include <fcntl.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/vfs.h>

#include <boost/bind.hpp>
#include <gflags/gflags.h>

#include "common/mutex.h"
#include "common/atomic.h"
#include "common/counter.h"
#include "common/mutex.h"
#include "common/thread_pool.h"
#include "common/util.h"
#include "common/timer.h"
#include "common/sliding_window.h"
#include "common/string_util.h"
#include "proto/nameserver.pb.h"
#include "rpc/rpc_client.h"

#include "chunkserver/counter_manager.h"
#include "chunkserver/data_block.h"
#include "chunkserver/block_manager.h"

// Avoid conflict, we define LOG...
#include "common/logging.h"

DECLARE_string(block_store_path);
DECLARE_string(nameserver);
DECLARE_string(nameserver_port);
DECLARE_string(chunkserver_port);
DECLARE_int32(heartbeat_interval);
DECLARE_int32(blockreport_interval);
DECLARE_int32(blockreport_size);
DECLARE_int32(write_buf_size);
DECLARE_int32(chunkserver_work_thread_num);
DECLARE_int32(chunkserver_read_thread_num);
DECLARE_int32(chunkserver_write_thread_num);
DECLARE_int32(chunkserver_max_pending_buffers);

namespace baidu {
namespace bfs {

extern common::Counter g_block_buffers;
extern common::Counter g_buffers_new;
extern common::Counter g_buffers_delete;
extern common::Counter g_blocks;
extern common::Counter g_writing_blocks;
extern common::Counter g_writing_bytes;
extern common::Counter g_read_ops;
extern common::Counter g_write_ops;
extern common::Counter g_write_bytes;
extern common::Counter g_refuse_ops;
extern common::Counter g_rpc_delay;
extern common::Counter g_rpc_delay_all;
extern common::Counter g_rpc_count;
extern common::Counter g_data_size;

const int kUnknownChunkServerId = -1;

ChunkServerImpl::ChunkServerImpl()
    : chunkserver_id_(kUnknownChunkServerId) {
    data_server_addr_ = common::util::GetLocalHostName() + ":" + FLAGS_chunkserver_port;
    work_thread_pool_ = new ThreadPool(FLAGS_chunkserver_work_thread_num);
    read_thread_pool_ = new ThreadPool(FLAGS_chunkserver_read_thread_num);
    write_thread_pool_ = new ThreadPool(FLAGS_chunkserver_write_thread_num);
    heartbeat_thread_ = new ThreadPool(1);
    block_manager_ = new BlockManager(write_thread_pool_, FLAGS_block_store_path);
    bool s_ret = block_manager_->LoadStorage();
    assert(s_ret == true);
    rpc_client_ = new RpcClient();
    std::string ns_address = FLAGS_nameserver + ":" + FLAGS_nameserver_port;
    if (!rpc_client_->GetStub(ns_address, &nameserver_)) {
        assert(0);
    }
    counter_manager_ = new CounterManager;
    work_thread_pool_->AddTask(boost::bind(&ChunkServerImpl::LogStatus, this, true));
    work_thread_pool_->AddTask(boost::bind(&ChunkServerImpl::SendBlockReport, this));
    heartbeat_thread_->AddTask(boost::bind(&ChunkServerImpl::SendHeartbeat, this));
}

ChunkServerImpl::~ChunkServerImpl() {
    work_thread_pool_->Stop(true);
    read_thread_pool_->Stop(true);
    write_thread_pool_->Stop(true);
    heartbeat_thread_->Stop(true);
    delete work_thread_pool_;
    delete read_thread_pool_;
    delete write_thread_pool_;
    delete heartbeat_thread_;
    delete block_manager_;
    delete rpc_client_;
    delete counter_manager_;
    LogStatus(false);
}

void ChunkServerImpl::LogStatus(bool routine) {
    counter_manager_->GatherCounters();
    CounterManager::Counters counters = counter_manager_->GetCounters();

    LOG(INFO, "[Status] blocks %ld %ld buffers %ld data %sB, "
              "find %ld read %ld write %ld %ld %.2f MB, rpc %ld %ld %ld",
        g_writing_blocks.Get() ,g_blocks.Get(), g_block_buffers.Get(),
        common::HumanReadableString(g_data_size.Get()).c_str(),
        counters.find_ops, counters.read_ops,
        counters.write_ops, counters.refuse_ops,
        counters.write_bytes / 1024.0 / 1024,
        counters.rpc_delay, counters.delay_all, work_thread_pool_->PendingNum());
    if (routine) {
        work_thread_pool_->DelayTask(1000,
            boost::bind(&ChunkServerImpl::LogStatus, this, true));
    }
}

void ChunkServerImpl::SendHeartbeat() {
    HeartBeatRequest request;
    request.set_chunkserver_id(chunkserver_id_);
    request.set_namespace_version(block_manager_->NameSpaceVersion());
    request.set_block_num(g_blocks.Get());
    request.set_data_size(g_data_size.Get());
    request.set_buffers(g_block_buffers.Get());
    HeartBeatResponse response;
    if (!rpc_client_->SendRequest(nameserver_, &NameServer_Stub::HeartBeat,
            &request, &response, 15, 1)) {
        LOG(WARNING, "Heat beat fail\n");
    } else if (block_manager_->NameSpaceVersion() != response.namespace_version()) {
        LOG(INFO, "Namespace version mismatch self:%ld ns:%ld",
            block_manager_->NameSpaceVersion(), response.namespace_version());
    }
    heartbeat_thread_->DelayTask(FLAGS_heartbeat_interval * 1000,
        boost::bind(&ChunkServerImpl::SendHeartbeat, this));
}

void ChunkServerImpl::SendBlockReport() {
    static int64_t last_report_blockid = -1;

    BlockReportRequest request;
    request.set_chunkserver_id(chunkserver_id_);
    request.set_chunkserver_addr(data_server_addr_);
    request.set_disk_quota(block_manager_->DiskQuota());
    request.set_namespace_version(block_manager_->NameSpaceVersion());

    std::vector<BlockMeta> blocks;
    block_manager_->ListBlocks(&blocks, last_report_blockid + 1, FLAGS_blockreport_size);

    int64_t blocks_num = blocks.size();
    for (int64_t i = 0; i < blocks_num; i++) {
        ReportBlockInfo* info = request.add_blocks();
        info->set_block_id(blocks[i].block_id);
        info->set_block_size(blocks[i].block_size);
        info->set_version(blocks[i].version);
    }

    if (blocks_num < FLAGS_blockreport_size) {
        last_report_blockid = -1;
        request.set_is_complete(true);
    } else {
        request.set_is_complete(false);
        if (blocks_num) {
            last_report_blockid = blocks[blocks_num - 1].block_id;
        }
    }

    BlockReportResponse response;
    if (!rpc_client_->SendRequest(nameserver_, &NameServer_Stub::BlockReport,
            &request, &response, 20, 3)) {
        LOG(WARNING, "Block reprot fail\n");
    } else {
        if (response.status() != 0) {
            LOG(FATAL, "Block report return %d", response.status());
        }
        int64_t new_version = response.namespace_version();
        if (block_manager_->NameSpaceVersion() != new_version) {
            // NameSpace change, chunkserver is empty.
            LOG(INFO, "New namespace version: %ld chunkserver id: %d",
                new_version, response.chunkserver_id());
            if (!block_manager_->SetNameSpaceVersion(new_version)) {
                LOG(FATAL, "Can not change namespace version");
            }
            chunkserver_id_ = response.chunkserver_id();
        } else if (chunkserver_id_ == kUnknownChunkServerId
                   && response.chunkserver_id() != kUnknownChunkServerId) {
            // Chunkserver restart
            chunkserver_id_ = response.chunkserver_id();
            LOG(INFO, "Reconnect to nameserver version= %ld, new cs_id = %d",
                block_manager_->NameSpaceVersion(), chunkserver_id_);
        } else if (response.chunkserver_id() == kUnknownChunkServerId) {
            // Namespace change, chunkserver has old blocks.
            LOG(INFO, "Old chunkserver, namespace version: %ld, old_id: %d",
                block_manager_->NameSpaceVersion(), chunkserver_id_);
        } else if (chunkserver_id_ != response.chunkserver_id()) {
            // Nameserver restart, chunkserver id change.
            LOG(INFO, "Chunkserver id change from %d to %d",
                chunkserver_id_, response.chunkserver_id());
            chunkserver_id_ = response.chunkserver_id();
        }
        //LOG(INFO, "Report return old: %d new: %d", chunkserver_id_, response.chunkserver_id());
        //deal with obsolete blocks
        std::vector<int64_t> obsolete_blocks;
        for (int i = 0; i < response.obsolete_blocks_size(); i++) {
            obsolete_blocks.push_back(response.obsolete_blocks(i));
        }
        if (!obsolete_blocks.empty()) {
            boost::function<void ()> task =
                boost::bind(&ChunkServerImpl::RemoveObsoleteBlocks,
                            this, obsolete_blocks);
            write_thread_pool_->AddTask(task);
        }

        std::vector<ReplicaInfo> new_replica_info;
        for (int i = 0; i < response.new_replicas_size(); i++) {
            new_replica_info.push_back(response.new_replicas(i));
        }
        LOG(INFO, "Block report done. %lu replica blocks", new_replica_info.size());
        if (!new_replica_info.empty()) {
            boost::function<void ()> new_replica_task =
                boost::bind(&ChunkServerImpl::PullNewBlocks,
                            this, new_replica_info);
            write_thread_pool_->AddTask(new_replica_task);
        }
    }
    work_thread_pool_->DelayTask(FLAGS_blockreport_interval* 1000,
        boost::bind(&ChunkServerImpl::SendBlockReport, this));
}

bool ChunkServerImpl::ReportFinish(Block* block) {
    BlockReportRequest request;
    request.set_chunkserver_id(chunkserver_id_);
    request.set_chunkserver_addr(data_server_addr_);
    request.set_namespace_version(block_manager_->NameSpaceVersion());
    request.set_is_complete(false);

    ReportBlockInfo* info = request.add_blocks();
    info->set_block_id(block->Id());
    info->set_block_size(block->Size());
    info->set_version(0);
    BlockReportResponse response;
    if (!rpc_client_->SendRequest(nameserver_, &NameServer_Stub::BlockReport,
            &request, &response, 20, 3)) {
        LOG(WARNING, "Reprot finish fail: %ld\n", block->Id());
        return false;
    }

    LOG(INFO, "Report finish to nameserver done, block_id: %ld\n", block->Id());
    return true;
}

void ChunkServerImpl::WriteBlock(::google::protobuf::RpcController* controller,
                        const WriteBlockRequest* request,
                        WriteBlockResponse* response,
                        ::google::protobuf::Closure* done) {
    int64_t block_id = request->block_id();
    const std::string& databuf = request->databuf();
    int64_t offset = request->offset();
    int32_t packet_seq = request->packet_seq();

    if (!response->has_sequence_id()) {
        response->set_sequence_id(request->sequence_id());
        /// Flow control
        if (g_block_buffers.Get() > FLAGS_chunkserver_max_pending_buffers
            || work_thread_pool_->PendingNum() > FLAGS_chunkserver_max_pending_buffers) {
            response->set_status(500);
            LOG(WARNING, "[WriteBlock] pending buf[%ld] req[%ld] reject #%ld seq:%d, offset:%ld, len:%lu ts:%lu\n",
                g_block_buffers.Get(), work_thread_pool_->PendingNum(),
                block_id, packet_seq, offset, databuf.size(), request->sequence_id());
            done->Run();
            g_refuse_ops.Inc();
            return;
        }
        LOG(DEBUG, "[WriteBlock] dispatch #%ld seq:%d, offset:%ld, len:%lu] %lu\n",
           block_id, packet_seq, offset, databuf.size(), request->sequence_id());
        response->add_timestamp(common::timer::get_micros());
        boost::function<void ()> task =
            boost::bind(&ChunkServerImpl::WriteBlock, this, controller, request, response, done);
        work_thread_pool_->AddTask(task);
        return;
    }

    response->add_timestamp(common::timer::get_micros());
    LOG(INFO, "[WriteBlock] #%ld seq:%d, offset:%ld, len:%lu",
           block_id, packet_seq, offset, databuf.size());

    if (request->chunkservers_size()) {
        // New request for next chunkserver
        WriteBlockRequest* next_request = new WriteBlockRequest(*request);
        WriteBlockResponse* next_response = new WriteBlockResponse();
        next_request->clear_chunkservers();
        for (int i = 1; i < request->chunkservers_size(); i++) {
            next_request->add_chunkservers(request->chunkservers(i));
        }
        ChunkServer_Stub* stub = NULL;
        const std::string& next_server = request->chunkservers(0);
        rpc_client_->GetStub(next_server, &stub);
        WriteNext(next_server, stub, next_request, next_response, request, response, done);
    } else {
        boost::function<void ()> callback =
            boost::bind(&ChunkServerImpl::LocalWriteBlock, this, request, response, done);
        work_thread_pool_->AddTask(callback);
    }
}

void ChunkServerImpl::WriteNext(const std::string& next_server,
                                ChunkServer_Stub* stub,
                                const WriteBlockRequest* next_request,
                                WriteBlockResponse* next_response,
                                const WriteBlockRequest* request,
                                WriteBlockResponse* response,
                                ::google::protobuf::Closure* done) {
    int64_t block_id = request->block_id();
    int32_t packet_seq = request->packet_seq();
    LOG(INFO, "[WriteBlock] send #%ld seq:%d to next %s\n",
        block_id, packet_seq, next_server.c_str());
    boost::function<void (const WriteBlockRequest*, WriteBlockResponse*, bool, int)> callback =
        boost::bind(&ChunkServerImpl::WriteNextCallback,
            this, _1, _2, _3, _4, next_server, std::make_pair(request, response), done, stub);
    rpc_client_->AsyncRequest(stub, &ChunkServer_Stub::WriteBlock,
        next_request, next_response, callback, 30, 3);
}

void ChunkServerImpl::WriteNextCallback(const WriteBlockRequest* next_request,
                        WriteBlockResponse* next_response,
                        bool failed, int error,
                        const std::string& next_server,
                        std::pair<const WriteBlockRequest*, WriteBlockResponse*> origin,
                        ::google::protobuf::Closure* done,
                        ChunkServer_Stub* stub) {
    const WriteBlockRequest* request = origin.first;
    WriteBlockResponse* response = origin.second;
    /// If RPC_ERROR_SEND_BUFFER_FULL retry send.
    if (failed && error == sofa::pbrpc::RPC_ERROR_SEND_BUFFER_FULL) {
        boost::function<void ()> callback =
            boost::bind(&ChunkServerImpl::WriteNext, this, next_server,
                        stub, next_request, next_response, request, response, done);
        work_thread_pool_->DelayTask(10, callback);
        return;
    }
    delete next_request;
    delete stub;

    int64_t block_id = request->block_id();
    const std::string& databuf = request->databuf();
    int64_t offset = request->offset();
    int32_t packet_seq = request->packet_seq();
    if (failed || next_response->status() != 0) {
        LOG(WARNING, "[WriteBlock] WriteNext %s fail: #%ld seq:%d, offset:%ld, len:%lu], "
                     "status= %d, error= %d\n",
            next_server.c_str(), block_id, packet_seq, offset, databuf.size(),
            next_response->status(), error);
        if (next_response->status() == 0) {
            response->set_status(error);
        } else {
            response->set_status(next_response->status());
        }
        delete next_response;
        done->Run();
        return;
    } else {
        LOG(INFO, "[Writeblock] send #%ld seq:%d to next done", block_id, packet_seq);
        delete next_response;
    }

    boost::function<void ()> callback =
        boost::bind(&ChunkServerImpl::LocalWriteBlock, this, request, response, done);
    work_thread_pool_->AddTask(callback);
}

void ChunkServerImpl::LocalWriteBlock(const WriteBlockRequest* request,
                        WriteBlockResponse* response,
                        ::google::protobuf::Closure* done) {
    int64_t block_id = request->block_id();
    const std::string& databuf = request->databuf();
    int64_t offset = request->offset();
    int32_t packet_seq = request->packet_seq();

    if (!response->has_status()) {
        response->set_status(0);
    }

    int64_t find_start = common::timer::get_micros();
    /// search;
    int64_t sync_time = 0;
    Block* block = block_manager_->FindBlock(block_id, true, &sync_time);
    if (!block) {
        LOG(WARNING, "[WriteBlock] Block not found: #%ld ", block_id);
        response->set_status(8404);
        done->Run();
        return;
    }

    int64_t add_used = 0;
    int64_t write_start = common::timer::get_micros();
    if (!block->Write(packet_seq, offset, databuf.data(), databuf.size(), &add_used)) {
        block->DecRef();
        response->set_status(812);
        done->Run();
        return;
    }
    int64_t write_end = common::timer::get_micros();
    if (request->is_last()) {
        block->SetSliceNum(packet_seq + 1);
        block->SetVersion(packet_seq);
    }

    // If complete, close block, and report only once(close block return true).
    int64_t report_start = write_end;
    if (block->IsComplete() && block_manager_->CloseBlock(block)) {
        LOG(INFO, "[WriteBlock] block finish #%ld size:%ld", block_id, block->Size());
        report_start = common::timer::get_micros();
        ReportFinish(block);
    }

    int64_t time_end = common::timer::get_micros();
    LOG(INFO, "[WriteBlock] done #%ld seq:%d, offset:%ld, len:%lu "
              "use %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld ms",
        block_id, packet_seq, offset, databuf.size(),
        (response->timestamp(0) - request->sequence_id()) / 1000, // recv
        (response->timestamp(1) - response->timestamp(0)) / 1000, // dispatch time
        (find_start - response->timestamp(1)) / 1000, // async time
        (write_start - find_start - sync_time) / 1000,  // find time
        sync_time / 1000, // create sync time
        add_used / 1000, // sliding window add
        (write_end - write_start) / 1000,    // write time
        (report_start - write_end) / 1000, // close time
        (time_end - report_start) / 1000, // report time
        (time_end - response->timestamp(0)) / 1000); // total time
    g_rpc_delay.Add(response->timestamp(0) - request->sequence_id());
    g_rpc_delay_all.Add(time_end - request->sequence_id());
    g_rpc_count.Inc();
    g_write_ops.Inc();
    done->Run();
    block->DecRef();
    block = NULL;
}

void ChunkServerImpl::ReadBlock(::google::protobuf::RpcController* controller,
                        const ReadBlockRequest* request,
                        ReadBlockResponse* response,
                        ::google::protobuf::Closure* done) {
    if (!response->has_sequence_id()) {
        response->set_sequence_id(request->sequence_id());
        response->add_timestamp(common::timer::get_micros());
        boost::function<void ()> task =
            boost::bind(&ChunkServerImpl::ReadBlock, this, controller, request, response, done);
        read_thread_pool_->AddTask(task);
        return;
    }

    int64_t block_id = request->block_id();
    int64_t offset = request->offset();
    int32_t read_len = request->read_len();
    int status = 0;

    int64_t find_start = common::timer::get_micros();
    Block* block = block_manager_->FindBlock(block_id, false);
    if (block == NULL) {
        status = 404;
        LOG(WARNING, "ReadBlock not found: #%ld offset: %ld len: %d\n",
                block_id, offset, read_len);
    } else {
        int64_t read_start = common::timer::get_micros();
        char* buf = new char[read_len];
        int32_t len = block->Read(buf, read_len, offset);
        int64_t read_end = common::timer::get_micros();
        if (len >= 0) {
            response->mutable_databuf()->assign(buf, len);
            if (request->require_block_version()) {
                response->set_block_version(block->GetVersion());
            }
            LOG(INFO, "ReadBlock #%ld offset: %ld len: %d return: %d "
                      "use %ld %ld %ld %ld %ld",
                block_id, offset, read_len, len,
                (response->timestamp(0) - request->sequence_id()) / 1000, // rpc time
                (find_start - response->timestamp(0)) / 1000,   // dispatch time
                (read_start - find_start) / 1000, // find time
                (read_end - read_start) / 1000,  // read time
                (read_end - response->timestamp(0)) / 1000);    // service time
            g_read_ops.Inc();
        } else {
            status = 882;
            LOG(WARNING, "ReadBlock #%ld fail offset: %ld len: %d\n",
                block_id, offset, read_len);
        }
        delete[] buf;
    }
    response->set_status(status);
    done->Run();
    if (block) {
        block->DecRef();
    }
}
void ChunkServerImpl::RemoveObsoleteBlocks(std::vector<int64_t> blocks) {
    for (size_t i = 0; i < blocks.size(); i++) {
        if (!block_manager_->RemoveBlock(blocks[i])) {
            LOG(INFO, "Remove block fail: #%ld ", blocks[i]);
        }
    }
}
void ChunkServerImpl::PullNewBlocks(std::vector<ReplicaInfo> new_replica_info) {
    PullBlockReportRequest report_request;
    report_request.set_sequence_id(0);
    report_request.set_chunkserver_id(chunkserver_id_);
    for (size_t i = 0; i < new_replica_info.size(); i++) {
        int64_t block_id = new_replica_info[i].block_id();
        ChunkServer_Stub* chunkserver = NULL;
        Block* block = block_manager_->FindBlock(block_id, true);
        if (!block) {
            LOG(WARNING, "Cant't create block: #%ld ", block_id);
            //ignore this block
            continue;
        } else {
            LOG(INFO, "Start pull #%ld from %s",
                block_id, new_replica_info[i].chunkserver_address(0).c_str());
        }
        int init_index = 0;
        for (; init_index < new_replica_info[i].chunkserver_address_size(); init_index++) {
            if (rpc_client_->GetStub(new_replica_info[i].chunkserver_address(init_index), &chunkserver)) {
                break;
            }
        }
        if (init_index == new_replica_info[i].chunkserver_address_size()) {
             LOG(WARNING, "Can't connect to any chunkservers for pull block #%ld\n", block_id);
            //remove this block
            block->DecRef();
            block_manager_->RemoveBlock(block_id);
            report_request.add_blocks(block_id);
            continue;
        }
        int64_t seq = -1;
        int64_t offset = 0;
        bool success = true;
        int pre_index = init_index;
        while (1) {
            ReadBlockRequest request;
            ReadBlockResponse response;
            request.set_sequence_id(++seq);
            request.set_block_id(block_id);
            request.set_offset(offset);
            request.set_read_len(256 * 1024);
            request.set_require_block_version(true);
            bool ret = rpc_client_->SendRequest(chunkserver,
                                                &ChunkServer_Stub::ReadBlock,
                                                &request, &response, 15, 3);
            if (!ret || response.status() != 0) {
                //try another chunkserver
                //reset seq
                --seq;
                delete chunkserver;
                pre_index = (pre_index + 1) % new_replica_info[i].chunkserver_address_size();
                LOG(INFO, "Change src chunkserver to %s for pull block #%ld",
                        new_replica_info[i].chunkserver_address(pre_index).c_str(), block_id);
                if (pre_index == init_index) {
                    success = false;
                    break;
                } else {
                    rpc_client_->GetStub(new_replica_info[i].chunkserver_address(pre_index), &chunkserver);
                    continue;
                }
            }
            int32_t len = response.databuf().size();
            const char* buf = response.databuf().data();
            if (len) {
                if (!block->Write(seq, offset, buf, len)) {
                    success = false;
                    break;
                }
            } else {
                block->SetSliceNum(seq);
                block->SetVersion(response.block_version());
            }
            if (block->IsComplete() && block_manager_->CloseBlock(block)) {
                LOG(INFO, "Pull block: #%ld finish\n", block_id);
                break;
            }
            offset += len;
        }
        delete chunkserver;
        block->DecRef();
        if (!success) {
            block_manager_->RemoveBlock(block_id);
        }
        report_request.add_blocks(block_id);
    }

    PullBlockReportResponse report_response;
    if (!rpc_client_->SendRequest(nameserver_, &NameServer_Stub::PullBlockReport,
                &report_request, &report_response, 15, 3)) {
        LOG(WARNING, "Report pull finish fail, chunkserver id: %d\n", chunkserver_id_);
    } else {
        LOG(INFO, "Report pull finish dnne, %d blocks", report_request.blocks_size());
    }
}


void ChunkServerImpl::GetBlockInfo(::google::protobuf::RpcController* controller,
                                   const GetBlockInfoRequest* request,
                                   GetBlockInfoResponse* response,
                                   ::google::protobuf::Closure* done) {
    if (!response->has_sequence_id()) {
        response->set_sequence_id(request->sequence_id());
        response->add_timestamp(common::timer::get_micros());
        boost::function<void ()> task =
            boost::bind(&ChunkServerImpl::GetBlockInfo, this, controller, request, response, done);
        read_thread_pool_->AddTask(task);
        return;
    }
    int64_t block_id = request->block_id();
    int status = 0;

    int64_t find_start = common::timer::get_micros();
    Block* block = block_manager_->FindBlock(block_id, false);
    int64_t find_end = common::timer::get_micros();
    if (block == NULL) {
        status = 404;
        LOG(WARNING, "GetBlockInfo not found: #%ld ",block_id);
    } else {
        int64_t block_size = block->GetMeta().block_size;
        response->set_block_size(block_size);
        status = 0;
        LOG(INFO, "GetBlockInfo #%ld return: %d "
                  "use %ld %ld %ld %ld",
            block_id, block_size,
            (response->timestamp(0) - request->sequence_id()) / 1000, // rpc time
            (find_start - response->timestamp(0)) / 1000,   // dispatch time
            (find_end - find_start) / 1000, // find time
            (find_end - response->timestamp(0)) / 1000);    // service time
    }
    response->set_status(status);
    done->Run();

    if (block) block->DecRef();

}

bool ChunkServerImpl::WebService(const sofa::pbrpc::HTTPRequest& request,
                                sofa::pbrpc::HTTPResponse& response) {
    CounterManager::Counters counters = counter_manager_->GetCounters();
    std::string str =
            "<html><head><title>BFS console</title>"
            "<meta http-equiv=\"Content-Type\" content=\"text/html; charset=utf-8\" />"
            "<link rel=\"stylesheet\" type=\"text/css\" "
                "href=\"http://www.w3school.com.cn/c5.css\"/>"
            "<style> body { background: #f9f9f9;}"
            "</style>"
            "</head>";
    str += "<body> <h1>分布式文件系统控制台 - ChunkServer</h1>";
    str += "<table class=dataintable>";
    str += "<tr><td>Block number</td><td>Data size</td>"
           "<td>Write(QPS)</td><td>Write(Speed)<td>Read(QPS)</td><td>Buffers(new/delete)</td><tr>";
    str += "<tr><td>" + common::NumToString(g_blocks.Get()) + "</td>";
    str += "<td>" + common::HumanReadableString(g_data_size.Get()) + "</td>";
    str += "<td>" + common::NumToString(counters.write_ops) + "</td>";
    str += "<td>" + common::HumanReadableString(counters.write_bytes) + "/S</td>";
    str += "<td>" + common::NumToString(counters.read_ops) + "</td>";
    str += "<td>" + common::NumToString(g_block_buffers.Get())
           + "(" + common::NumToString(counters.buffers_new) + "/"
           + common::NumToString(counters.buffers_delete) +")" + "</td>";
    str += "</tr>";
    str += "</table>";
    str += "<script> var int = setInterval('window.location.reload()', 1000);"
           "function check(box) {"
           "if(box.checked) {"
           "    int = setInterval('window.location.reload()', 1000);"
           "} else {"
           "    clearInterval(int);"
           "}"
           "}</script>"
           "<input onclick=\"javascript:check(this)\" "
           "checked=\"checked\" type=\"checkbox\">自动刷新</input>";
    str += "</body></html>";
    response.content = str;
    return true;
}
} // namespace bfs
} //namespace baidu

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
