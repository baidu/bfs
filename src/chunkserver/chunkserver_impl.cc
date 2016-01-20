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

#include <common/mutex.h>
#include <common/atomic.h>
#include <common/counter.h>
#include <common/mutex.h>
#include <common/thread_pool.h>
#include <common/util.h>
#include <common/timer.h>
#include <common/sliding_window.h>
#include <common/string_util.h>
#include "proto/nameserver.pb.h"
#include "proto/status_code.pb.h"
#include "rpc/rpc_client.h"

#include "chunkserver/counter_manager.h"
#include "chunkserver/data_block.h"
#include "chunkserver/block_manager.h"

// Avoid conflict, we define LOG...
#include <common/logging.h>

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
DECLARE_int32(chunkserver_recover_thread_num);
DECLARE_int32(chunkserver_max_pending_buffers);
DECLARE_bool(chunkserver_auto_clean);

namespace baidu {
namespace bfs {

extern common::Counter g_block_buffers;
extern common::Counter g_buffers_new;
extern common::Counter g_buffers_delete;
extern common::Counter g_blocks;
extern common::Counter g_writing_blocks;
extern common::Counter g_writing_bytes;
extern common::Counter g_read_ops;
extern common::Counter g_read_bytes;
extern common::Counter g_write_ops;
extern common::Counter g_write_bytes;
extern common::Counter g_recover_bytes;
extern common::Counter g_refuse_ops;
extern common::Counter g_rpc_delay;
extern common::Counter g_rpc_delay_all;
extern common::Counter g_rpc_count;
extern common::Counter g_data_size;

ChunkServerImpl::ChunkServerImpl()
    : chunkserver_id_(kUnknownChunkServerId),
     heartbeat_task_id_(-1),
     blockreport_task_id_(-1),
     last_report_blockid_(-1) {
    data_server_addr_ = common::util::GetLocalHostName() + ":" + FLAGS_chunkserver_port;
    work_thread_pool_ = new ThreadPool(FLAGS_chunkserver_work_thread_num);
    read_thread_pool_ = new ThreadPool(FLAGS_chunkserver_read_thread_num);
    write_thread_pool_ = new ThreadPool(FLAGS_chunkserver_write_thread_num);
    recover_thread_pool_ = new ThreadPool(FLAGS_chunkserver_recover_thread_num);
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
    heartbeat_thread_->AddTask(boost::bind(&ChunkServerImpl::Register, this));
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
    LogStatus(false);
    delete counter_manager_;
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

void ChunkServerImpl::Register() {
    RegisterRequest request;
    request.set_chunkserver_addr(data_server_addr_);
    request.set_disk_quota(block_manager_->DiskQuota());
    request.set_namespace_version(block_manager_->NameSpaceVersion());

    LOG(INFO, "Send Register request with version %ld", request.namespace_version());
    RegisterResponse response;
    if (!rpc_client_->SendRequest(nameserver_, &NameServer_Stub::Register,
            &request, &response, 20, 3)) {
        LOG(WARNING, "Register fail, wait and retry");
        work_thread_pool_->DelayTask(5000, boost::bind(&ChunkServerImpl::Register, this));
        return;
    }
    if (response.status() != kOK) {
        LOG(FATAL, "Block report return %d", response.status());
        work_thread_pool_->DelayTask(5000, boost::bind(&ChunkServerImpl::Register, this));
        return;
    }
    int64_t new_version = response.namespace_version();
    if (block_manager_->NameSpaceVersion() != new_version) {
        // NameSpace change
        if (!FLAGS_chunkserver_auto_clean) {
            /// abort
            LOG(FATAL, "Name space verion FLAGS_chunkserver_auto_clean == false");
        }
        LOG(INFO, "Use new namespace version: %ld, clean local data", new_version);
        // Clean
        if (!block_manager_->RemoveAllBlocks()) {
            LOG(FATAL, "Remove local blocks fail");
        }
        if (!block_manager_->SetNameSpaceVersion(new_version)) {
            LOG(FATAL, "SetNameSpaceVersion fail");
        }
        work_thread_pool_->AddTask(boost::bind(&ChunkServerImpl::Register, this));
        return;
    }
    assert (response.chunkserver_id() != kUnknownChunkServerId);
    chunkserver_id_ = response.chunkserver_id();
    LOG(INFO, "Connect to nameserver version= %ld, cs_id = %d",
        block_manager_->NameSpaceVersion(), chunkserver_id_);

    work_thread_pool_->DelayTask(1, boost::bind(&ChunkServerImpl::SendBlockReport, this));
    heartbeat_thread_->DelayTask(1, boost::bind(&ChunkServerImpl::SendHeartbeat, this));
}

void ChunkServerImpl::StopBlockReport() {
    int64_t now_reprot_id = blockreport_task_id_;
    work_thread_pool_->CancelTask(now_reprot_id);
    while (now_reprot_id != blockreport_task_id_) {
        now_reprot_id = blockreport_task_id_;
        work_thread_pool_->CancelTask(blockreport_task_id_);
    }
}

void ChunkServerImpl::SendHeartbeat() {
    HeartBeatRequest request;
    request.set_chunkserver_id(chunkserver_id_);
    request.set_namespace_version(block_manager_->NameSpaceVersion());
    request.set_chunkserver_addr(data_server_addr_);
    request.set_block_num(g_blocks.Get());
    request.set_data_size(g_data_size.Get());
    request.set_buffers(g_block_buffers.Get());
    HeartBeatResponse response;
    if (!rpc_client_->SendRequest(nameserver_, &NameServer_Stub::HeartBeat,
            &request, &response, 15, 1)) {
        LOG(WARNING, "Heart beat fail\n");
    } else if (response.status() != kOK) {
        if (block_manager_->NameSpaceVersion() != response.namespace_version()) {
            LOG(INFO, "Namespace version mismatch self:%ld ns:%ld",
                block_manager_->NameSpaceVersion(), response.namespace_version());
        } else {
            LOG(INFO, "Nameserver restart!");
        }
        StopBlockReport();
        heartbeat_thread_->AddTask(boost::bind(&ChunkServerImpl::Register, this));
        return;
    }
    heartbeat_task_id_ = heartbeat_thread_->DelayTask(FLAGS_heartbeat_interval * 1000,
        boost::bind(&ChunkServerImpl::SendHeartbeat, this));
}

void ChunkServerImpl::SendBlockReport() {
    BlockReportRequest request;
    request.set_chunkserver_id(chunkserver_id_);
    request.set_chunkserver_addr(data_server_addr_);

    std::vector<BlockMeta> blocks;
    block_manager_->ListBlocks(&blocks, last_report_blockid_ + 1, FLAGS_blockreport_size);

    int64_t blocks_num = blocks.size();
    for (int64_t i = 0; i < blocks_num; i++) {
        ReportBlockInfo* info = request.add_blocks();
        info->set_block_id(blocks[i].block_id);
        info->set_block_size(blocks[i].block_size);
        info->set_version(blocks[i].version);
    }

    if (blocks_num < FLAGS_blockreport_size) {
        last_report_blockid_ = -1;
    } else {
        if (blocks_num) {
            last_report_blockid_ = blocks[blocks_num - 1].block_id;
        }
    }

    BlockReportResponse response;
    if (!rpc_client_->SendRequest(nameserver_, &NameServer_Stub::BlockReport,
            &request, &response, 20, 3)) {
        LOG(WARNING, "Block reprot fail\n");
    } else {
        if (response.status() != kOK) {
            last_report_blockid_ = -1;
            LOG(WARNING, "BlockReport return %d, Pause to report", response.status());
            return;
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

        LOG(INFO, "Block report done. %d replica blocks", response.new_replicas_size());
        for (int i = 0; i < response.new_replicas_size(); i++) {
            boost::function<void ()> new_replica_task =
                boost::bind(&ChunkServerImpl::PullNewBlock, this, response.new_replicas(i));
            recover_thread_pool_->AddTask(new_replica_task);
        }
    }
    blockreport_task_id_ = work_thread_pool_->DelayTask(FLAGS_blockreport_interval* 1000,
        boost::bind(&ChunkServerImpl::SendBlockReport, this));
}

bool ChunkServerImpl::ReportFinish(Block* block) {
    BlockReceivedRequest request;
    request.set_chunkserver_id(chunkserver_id_);
    request.set_chunkserver_addr(data_server_addr_);

    ReportBlockInfo* info = request.add_blocks();
    info->set_block_id(block->Id());
    info->set_block_size(block->Size());
    info->set_version(block->GetVersion());
    BlockReceivedResponse response;
    if (!rpc_client_->SendRequest(nameserver_, &NameServer_Stub::BlockReceived,
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
        response->set_status(kOK);
    }

    int64_t find_start = common::timer::get_micros();
    /// search;
    int64_t sync_time = 0;
    Block* block = block_manager_->FindBlock(block_id, true, &sync_time);
    if (!block) {
        LOG(WARNING, "[WriteBlock] Block not found: #%ld ", block_id);
        response->set_status(kNotFound);
        done->Run();
        return;
    }

    int64_t add_used = 0;
    int64_t write_start = common::timer::get_micros();
    if (!block->Write(packet_seq, offset, databuf.data(), databuf.size(), &add_used)) {
        block->DecRef();
        response->set_status(kWriteError);
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
    int64_t block_id = request->block_id();
    int64_t offset = request->offset();
    int32_t read_len = request->read_len();
    if (read_len <= 0 || read_len > (64<<20) || offset < 0) {
        LOG(WARNING, "ReadBlock bad parameters %d %ld", read_len, offset);
        response->set_status(kBadParameter);
        done->Run();
        return;
    }
    if (!response->has_sequence_id()) {
        response->set_sequence_id(request->sequence_id());
        response->add_timestamp(common::timer::get_micros());
        boost::function<void ()> task =
            boost::bind(&ChunkServerImpl::ReadBlock, this, controller, request, response, done);
        read_thread_pool_->AddTask(task);
        return;
    }

    int status = kOK;

    int64_t find_start = common::timer::get_micros();
    Block* block = block_manager_->FindBlock(block_id, false);
    if (block == NULL) {
        status = kNotFound;
        LOG(WARNING, "ReadBlock not found: #%ld offset: %ld len: %d\n",
                block_id, offset, read_len);
    } else {
        int64_t read_start = common::timer::get_micros();
        char* buf = new char[read_len];
        int64_t len = block->Read(buf, read_len, offset);
        int64_t read_end = common::timer::get_micros();
        if (len >= 0) {
            response->mutable_databuf()->assign(buf, len);
            if (request->require_block_version()) {
                response->set_block_version(block->GetVersion());
            }
            LOG(INFO, "ReadBlock #%ld offset: %ld len: %ld return: %ld "
                      "use %ld %ld %ld %ld %ld",
                block_id, offset, read_len, len,
                (response->timestamp(0) - request->sequence_id()) / 1000, // rpc time
                (find_start - response->timestamp(0)) / 1000,   // dispatch time
                (read_start - find_start) / 1000, // find time
                (read_end - read_start) / 1000,  // read time
                (read_end - response->timestamp(0)) / 1000);    // service time
            g_read_ops.Inc();
            g_read_bytes.Add(len);
        } else {
            status = kReadError;
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
void ChunkServerImpl::PullNewBlock(const ReplicaInfo& new_replica_info) {
    PullBlockReportRequest report_request;
    report_request.set_sequence_id(0);
    report_request.set_chunkserver_id(chunkserver_id_);
    int64_t block_id = new_replica_info.block_id();
    LOG(INFO, "started pull : #%ld ", block_id);

    int64_t seq = -1;
    int64_t offset = 0;
    bool success = true;
    int init_index = 0;
    int pre_index = -1;
    ChunkServer_Stub* chunkserver = NULL;
    Block* block = block_manager_->FindBlock(block_id, false);
    if (block) {
        block->DecRef();
        LOG(INFO, "already has #%ld , skip pull", block_id);
        report_request.add_failed(block_id);
        goto REPORT;
    }
    block = block_manager_->FindBlock(block_id, true);
    if (!block) {
        LOG(WARNING, "Cant't create block: #%ld ", block_id);
        //ignore this block
        report_request.add_failed(block_id);
        goto REPORT;
    } else {
        LOG(INFO, "Start pull #%ld from %s",
            block_id, new_replica_info.chunkserver_address(0).c_str());
    }
    for (; init_index < new_replica_info.chunkserver_address_size(); init_index++) {
        if (rpc_client_->GetStub(new_replica_info.chunkserver_address(init_index), &chunkserver)) {
            break;
        }
    }
    if (init_index == new_replica_info.chunkserver_address_size()) {
         LOG(WARNING, "Can't connect to any chunkservers for pull block #%ld ", block_id);
        //remove this block
        block->DecRef();
        block_manager_->RemoveBlock(block_id);
        report_request.add_failed(block_id);
        goto REPORT;
    }
    pre_index = init_index;
    while (1) {
        ReadBlockRequest request;
        ReadBlockResponse response;
        request.set_sequence_id(++seq);
        request.set_block_id(block_id);
        request.set_offset(offset);
        request.set_read_len(1 << 20);
        request.set_require_block_version(true);
        bool ret = rpc_client_->SendRequest(chunkserver,
                                            &ChunkServer_Stub::ReadBlock,
                                            &request, &response, 15, 3);
        if (!ret || response.status() != kOK) {
            //try another chunkserver
            //reset seq
            --seq;
            delete chunkserver;
            chunkserver = NULL;
            pre_index = (pre_index + 1) % new_replica_info.chunkserver_address_size();
            LOG(INFO, "Change src chunkserver to %s for pull block #%ld ",
                    new_replica_info.chunkserver_address(pre_index).c_str(), block_id);
            if (pre_index == init_index) {
                success = false;
                break;
            } else {
                rpc_client_->GetStub(new_replica_info.chunkserver_address(pre_index), &chunkserver);
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
            g_recover_bytes.Add(len);
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
        report_request.add_failed(block_id);
    } else {
        report_request.add_blocks(block_id);
    }
    LOG(INFO, "done pull : #%ld %ld bytes", block_id, offset);

REPORT:
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
    int status = kOK;

    int64_t find_start = common::timer::get_micros();
    Block* block = block_manager_->FindBlock(block_id, false);
    int64_t find_end = common::timer::get_micros();
    if (block == NULL) {
        status = kNotFound;
        LOG(WARNING, "GetBlockInfo not found: #%ld ",block_id);
    } else {
        int64_t block_size = block->GetMeta().block_size;
        response->set_block_size(block_size);
        status = kOK;
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
           "<td>Write(QPS)</td><td>Write(Speed)</td><td>Read(QPS)</td><td>Read(Speed)</td>"
           "<td>Recover(Speed)</td><td>Buffers(new/delete)</td><tr>";
    str += "<tr><td>" + common::NumToString(g_blocks.Get()) + "</td>";
    str += "<td>" + common::HumanReadableString(g_data_size.Get()) + "</td>";
    str += "<td>" + common::NumToString(counters.write_ops) + "</td>";
    str += "<td>" + common::HumanReadableString(counters.write_bytes) + "/S</td>";
    str += "<td>" + common::NumToString(counters.read_ops) + "</td>";
    str += "<td>" + common::HumanReadableString(counters.read_bytes) + "/S</td>";
    str += "<td>" + common::HumanReadableString(counters.recover_bytes) + "/S</td>";
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
