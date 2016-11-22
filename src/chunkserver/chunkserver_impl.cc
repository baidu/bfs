// Copyright (c) 2014, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#include "chunkserver_impl.h"

#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/vfs.h>
#include <climits>
#include <functional>

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
#include "rpc/nameserver_client.h"

#include "chunkserver/counter_manager.h"
#include "chunkserver/data_block.h"
#include "chunkserver/block_manager.h"

// Avoid conflict, we define LOG...
#include <common/logging.h>

DECLARE_string(block_store_path);
DECLARE_string(nameserver_nodes);
DECLARE_string(chunkserver_port);
DECLARE_string(chunkserver_tag);
DECLARE_int32(heartbeat_interval);
DECLARE_int32(blockreport_interval);
DECLARE_int32(blockreport_size);
DECLARE_int32(write_buf_size);
DECLARE_int32(chunkserver_work_thread_num);
DECLARE_int32(chunkserver_read_thread_num);
DECLARE_int32(chunkserver_write_thread_num);
DECLARE_int32(chunkserver_recover_thread_num);
DECLARE_int32(chunkserver_max_pending_buffers);
DECLARE_int64(chunkserver_max_unfinished_bytes);
DECLARE_bool(chunkserver_auto_clean);
DECLARE_int32(block_report_timeout);

namespace baidu {
namespace bfs {

extern common::Counter g_block_buffers;
extern common::Counter g_buffers_new;
extern common::Counter g_buffers_delete;
extern common::Counter g_blocks;
extern common::Counter g_writing_blocks;
extern common::Counter g_pending_writes;
extern common::Counter g_unfinished_bytes;
extern common::Counter g_writing_bytes;
extern common::Counter g_read_ops;
extern common::Counter g_read_bytes;
extern common::Counter g_write_ops;
extern common::Counter g_write_bytes;
extern common::Counter g_recover_bytes;
extern common::Counter g_recover_count;
extern common::Counter g_refuse_ops;
extern common::Counter g_rpc_delay;
extern common::Counter g_rpc_delay_all;
extern common::Counter g_rpc_count;
extern common::Counter g_data_size;

ChunkServerImpl::ChunkServerImpl()
    : chunkserver_id_(-1),
     heartbeat_task_id_(-1),
     blockreport_task_id_(-1),
     last_report_blockid_(-1),
     report_id_(0),
     is_first_round_(true),
     first_round_report_start_(-1),
     service_stop_(false) {
    data_server_addr_ = common::util::GetLocalHostName() + ":" + FLAGS_chunkserver_port;
    params_.set_report_interval(FLAGS_blockreport_interval);
    params_.set_report_size(FLAGS_blockreport_size);
    work_thread_pool_ = new ThreadPool(FLAGS_chunkserver_work_thread_num);
    read_thread_pool_ = new ThreadPool(FLAGS_chunkserver_read_thread_num);
    write_thread_pool_ = new ThreadPool(FLAGS_chunkserver_write_thread_num);
    recover_thread_pool_ = new ThreadPool(FLAGS_chunkserver_recover_thread_num);
    heartbeat_thread_ = new ThreadPool(1);
    block_manager_ = new BlockManager(FLAGS_block_store_path);
    bool s_ret = block_manager_->LoadStorage();
    assert(s_ret == true);
    rpc_client_ = new RpcClient();
    nameserver_ = new NameServerClient(rpc_client_, FLAGS_nameserver_nodes);
    counter_manager_ = new CounterManager;
    heartbeat_thread_->AddTask(std::bind(&ChunkServerImpl::LogStatus, this, true));
    heartbeat_thread_->AddTask(std::bind(&ChunkServerImpl::Register, this));
}

ChunkServerImpl::~ChunkServerImpl() {
    service_stop_ = true;
    recover_thread_pool_->Stop(true);
    work_thread_pool_->Stop(true);
    read_thread_pool_->Stop(true);
    write_thread_pool_->Stop(true);
    heartbeat_thread_->Stop(true);
    delete block_manager_;
    delete rpc_client_;
    LogStatus(false);
    delete counter_manager_;
    delete recover_thread_pool_;
    delete work_thread_pool_;
    delete read_thread_pool_;
    delete write_thread_pool_;
    delete heartbeat_thread_;
}

void ChunkServerImpl::LogStatus(bool routine) {
    counter_manager_->GatherCounters();
    CounterManager::Counters counters = counter_manager_->GetCounters();

    LOG(INFO, "[Status] blocks %ld %ld buffers %ld pending %ld data %sB, "
              "find %ld read %ld write %ld %ld %.2f MB, rpc %ld %ld %ld, "
              "unfinished: %ld recovering %ld",
        g_writing_blocks.Get() ,g_blocks.Get(), g_block_buffers.Get(), g_pending_writes.Get(),
        common::HumanReadableString(g_data_size.Get()).c_str(),
        counters.find_ops, counters.read_ops,
        counters.write_ops, counters.refuse_ops,
        counters.write_bytes / 1024.0 / 1024,
        counters.rpc_delay, counters.delay_all, work_thread_pool_->PendingNum(),
        counters.unfinished_write_bytes, g_recover_count.Get());
    if (routine) {
        heartbeat_thread_->DelayTask(1000,
            std::bind(&ChunkServerImpl::LogStatus, this, true));
    }
}

void ChunkServerImpl::Register() {
    RegisterRequest request;
    request.set_chunkserver_addr(data_server_addr_);
    request.set_disk_quota(block_manager_->DiskQuota());
    request.set_namespace_version(block_manager_->NameSpaceVersion());
    request.set_tag(FLAGS_chunkserver_tag);

    LOG(INFO, "Send Register request with version %ld ", request.namespace_version());
    RegisterResponse response;
    if (!nameserver_->SendRequest(&NameServer_Stub::Register, &request, &response, 20)) {
        LOG(WARNING, "Register fail, wait and retry");
        work_thread_pool_->DelayTask(5000, std::bind(&ChunkServerImpl::Register, this));
        return;
    }
    if (response.status() != kOK) {
        LOG(WARNING, "Register return %s", StatusCode_Name(response.status()).c_str());
        work_thread_pool_->DelayTask(5000, std::bind(&ChunkServerImpl::Register, this));
        return;
    }
    if (response.report_interval() != -1) {
        params_.set_report_interval(response.report_interval());
    }
    if (response.report_size() != -1) {
        params_.set_report_size(response.report_size());
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
        work_thread_pool_->AddTask(std::bind(&ChunkServerImpl::Register, this));
        return;
    }
    assert (response.chunkserver_id() != -1);
    chunkserver_id_ = response.chunkserver_id();
    report_id_ = response.report_id() + 1;
    first_round_report_start_ = last_report_blockid_;
    is_first_round_ = true;
    LOG(INFO, "Connect to nameserver version= %ld, cs_id = C%d report_interval = %d "
            "report_size = %d report_id = %ld",
            block_manager_->NameSpaceVersion(), chunkserver_id_,
            params_.report_interval(), params_.report_size(), report_id_);

    work_thread_pool_->DelayTask(1, std::bind(&ChunkServerImpl::SendBlockReport, this));
    heartbeat_thread_->DelayTask(1, std::bind(&ChunkServerImpl::SendHeartbeat, this));
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
    CounterManager::Counters counters = counter_manager_->GetCounters();
    request.set_chunkserver_id(chunkserver_id_);
    request.set_namespace_version(block_manager_->NameSpaceVersion());
    request.set_chunkserver_addr(data_server_addr_);
    request.set_block_num(g_blocks.Get());
    request.set_data_size(g_data_size.Get());
    request.set_buffers(g_block_buffers.Get());
    request.set_pending_writes(g_pending_writes.Get());
    request.set_pending_recover(g_recover_count.Get());
    request.set_w_qps(counters.write_ops);
    request.set_w_speed(counters.write_bytes);
    request.set_r_qps(counters.read_ops);
    request.set_r_speed(counters.read_bytes);
    request.set_recover_speed(counters.recover_bytes);
    HeartBeatResponse response;
    if (!nameserver_->SendRequest(&NameServer_Stub::HeartBeat, &request, &response, 15)) {
        LOG(WARNING, "Heart beat fail\n");
    } else if (response.status() != kOK) {
        if (block_manager_->NameSpaceVersion() != response.namespace_version()) {
            LOG(INFO, "Namespace version mismatch self:%ld ns:%ld",
                block_manager_->NameSpaceVersion(), response.namespace_version());
        } else {
            LOG(INFO, "Nameserver restart!");
        }
        StopBlockReport();
        heartbeat_thread_->AddTask(std::bind(&ChunkServerImpl::Register, this));
        return;
    } else if (response.kick()) {
        LOG(WARNING, "Kick by nameserver");
        kill(getpid(), SIGTERM);
        return;
    }
    if (response.report_interval() != -1) {
        params_.set_report_interval(response.report_interval());
    }
    if (response.report_size() != -1) {
        params_.set_report_size(response.report_size());
    }
    heartbeat_task_id_ = heartbeat_thread_->DelayTask(FLAGS_heartbeat_interval * 1000,
        std::bind(&ChunkServerImpl::SendHeartbeat, this));
}

void ChunkServerImpl::SendBlockReport() {
    BlockReportRequest request;
    request.set_sequence_id(common::timer::get_micros());
    request.set_chunkserver_id(chunkserver_id_);
    request.set_chunkserver_addr(data_server_addr_);
    request.set_start(last_report_blockid_ + 1);
    request.set_report_id(report_id_);
    int64_t last_report_id = report_id_;

    std::vector<BlockMeta> blocks;
    int32_t num = is_first_round_ ? 10000 : params_.report_size();
    int64_t end = block_manager_->ListBlocks(&blocks, last_report_blockid_ + 1, num);
    // last id + 1 <= first found report start <= end -> first round ends
    if (is_first_round_ &&
        (last_report_blockid_ + 1) <= first_round_report_start_ &&
        first_round_report_start_ <= end) {
        is_first_round_ = false;
        LOG(INFO, "First round report done");
    }
    // bugfix, need an elegant implementation T_T
    if (is_first_round_ && first_round_report_start_ == -1) {
        first_round_report_start_ = 0;
    }

    int64_t blocks_num = blocks.size();
    for (int64_t i = 0; i < blocks_num; i++) {
        ReportBlockInfo* info = request.add_blocks();
        info->set_block_id(blocks[i].block_id());
        info->set_block_size(blocks[i].block_size());
        info->set_version(blocks[i].version());
    }

    if (blocks_num < num) {
        last_report_blockid_ = -1;
        end = LLONG_MAX;
    } else {
        last_report_blockid_ = end;
    }
    request.set_end(end);

    BlockReportResponse response;
    common::timer::TimeChecker checker;
    bool ret = nameserver_->SendRequest(&NameServer_Stub::BlockReport,
                                        &request, &response, FLAGS_block_report_timeout);
    checker.Check(20 * 1000 * 1000, "[SendBlockReport] SendRequest");
    if (!ret) {
        LOG(WARNING, "Block report fail last_id %lu (%lu)\n", last_report_id, request.sequence_id());
    } else {
        if (response.status() != kOK) {
            last_report_blockid_ = -1;
            report_id_ = 0;
            LOG(WARNING, "BlockReport return %s, Pause to report", StatusCode_Name(response.status()).c_str());
            return;
        }
        //LOG(INFO, "Report return old: %d new: %d", chunkserver_id_, response.chunkserver_id());
        //deal with obsolete blocks
        report_id_ = response.report_id() + 1;
        std::vector<int64_t> obsolete_blocks;
        for (int i = 0; i < response.obsolete_blocks_size(); i++) {
            obsolete_blocks.push_back(response.obsolete_blocks(i));
        }
        if (!obsolete_blocks.empty()) {
            std::function<void ()> task =
                std::bind(&ChunkServerImpl::RemoveObsoleteBlocks,
                            this, obsolete_blocks);
            write_thread_pool_->AddTask(task);
        }

        LOG(INFO, "Block report (%lu) done. %d replica blocks last_id %ld next_id %ld",
                request.sequence_id(), response.new_replicas_size(), last_report_id, report_id_);
        g_recover_count.Add(response.new_replicas_size());
        for (int i = 0; i < response.new_replicas_size(); ++i) {
            const ReplicaInfo& rep = response.new_replicas(i);
            int32_t cancel_time = common::timer::now_time() + rep.recover_timeout();
            std::function<void ()> new_replica_task =
                std::bind(&ChunkServerImpl::PushBlock, this, rep, cancel_time);
            LOG(INFO, "schedule push #%ld ", rep.block_id());
            if (rep.priority()) {
                recover_thread_pool_->AddPriorityTask(new_replica_task);
            } else {
                recover_thread_pool_->AddTask(new_replica_task);
            }
        }

        for (int i = 0; i < response.close_blocks_size(); ++i) {
            std::function<void ()> close_block_task = // TODO
                std::bind(&ChunkServerImpl::CloseIncompleteBlock, this, response.close_blocks(i));
            write_thread_pool_->AddTask(close_block_task);
        }
    }
    blockreport_task_id_ = work_thread_pool_->DelayTask(params_.report_interval() * 1000,
        std::bind(&ChunkServerImpl::SendBlockReport, this));
}

bool ChunkServerImpl::ReportFinish(Block* block) {
    BlockReceivedRequest request;
    request.set_chunkserver_id(chunkserver_id_);
    request.set_chunkserver_addr(data_server_addr_);

    ReportBlockInfo* info = request.add_blocks();
    info->set_block_id(block->Id());
    info->set_block_size(block->Size());
    info->set_version(block->GetVersion());
    info->set_is_recover(block->IsRecover());
    BlockReceivedResponse response;
    if (!nameserver_->SendRequest(&NameServer_Stub::BlockReceived, &request, &response, 20)) {
        LOG(WARNING, "Reprot finish fail: #%ld ", block->Id());
        return false;
    }

    LOG(INFO, "Report finish to nameserver done, block_id: #%ld ", block->Id());
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

    if (!response->has_sequence_id() &&
            g_unfinished_bytes.Add(databuf.size()) > FLAGS_chunkserver_max_unfinished_bytes) {
        response->set_sequence_id(request->sequence_id());
        g_refuse_ops.Inc();
        LOG(WARNING, "[WriteBlock] Too much unfinished write request(%ld), reject #%ld seq:%d offset:%ld len:%lu ts%lu",
                g_unfinished_bytes.Get(), block_id, packet_seq, offset, databuf.size(), request->sequence_id());
        response->set_status(kCsTooMuchUnfinishedWrite);
        g_unfinished_bytes.Sub(databuf.size());
        done->Run();
        return;
    }
    if (!response->has_sequence_id()) {
        response->set_sequence_id(request->sequence_id());
        /// Flow control
        if (g_block_buffers.Get() > FLAGS_chunkserver_max_pending_buffers) {
            response->set_status(kCsTooMuchPendingBuffer);
            LOG(WARNING, "[WriteBlock] pending buf[%ld] req[%ld] reject #%ld seq:%d, offset:%ld, len:%lu ts:%lu\n",
                g_block_buffers.Get(), work_thread_pool_->PendingNum(),
                block_id, packet_seq, offset, databuf.size(), request->sequence_id());
            g_unfinished_bytes.Sub(databuf.size());
            done->Run();
            g_refuse_ops.Inc();
            return;
        }
        LOG(DEBUG, "[WriteBlock] dispatch #%ld seq:%d, offset:%ld, len:%lu ts:%lu\n",
           block_id, packet_seq, offset, databuf.size(), request->sequence_id());
        response->add_timestamp(common::timer::get_micros());
        std::function<void ()> task =
            std::bind(&ChunkServerImpl::WriteBlock, this, controller, request, response, done);
        work_thread_pool_->AddTask(task);
        return;
    }

    response->add_timestamp(common::timer::get_micros());
    LOG(INFO, "[WriteBlock] #%ld seq:%d, offset:%ld, len:%lu",
           block_id, packet_seq, offset, databuf.size());

    int next_cs_offset = -1;
    for (int i = 0; i < request->chunkservers_size(); i++) {
        if (request->chunkservers(i) == data_server_addr_) {
            next_cs_offset = i + 1;
            break;
        }
    }
    if (next_cs_offset >= 0 && next_cs_offset < request->chunkservers_size()) {
        // share same write request
        const WriteBlockRequest* next_request = request;
        WriteBlockResponse* next_response = new WriteBlockResponse();
        ChunkServer_Stub* stub = NULL;
        const std::string& next_server = request->chunkservers(next_cs_offset);
        rpc_client_->GetStub(next_server, &stub);
        WriteNext(next_server, stub, next_request, next_response, request, response, done);
    } else {
        std::function<void ()> callback =
            std::bind(&ChunkServerImpl::LocalWriteBlock, this, request, response, done);
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
    std::function<void (const WriteBlockRequest*, WriteBlockResponse*, bool, int)> callback =
        std::bind(&ChunkServerImpl::WriteNextCallback,
            this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3,
            std::placeholders::_4, next_server, std::make_pair(request, response), done, stub);
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
        std::function<void ()> callback =
            std::bind(&ChunkServerImpl::WriteNext, this, next_server,
                        stub, next_request, next_response, request, response, done);
        work_thread_pool_->DelayTask(10, callback);
        return;
    }
    delete stub;

    int64_t block_id = request->block_id();
    const std::string& databuf = request->databuf();
    int64_t offset = request->offset();
    int32_t packet_seq = request->packet_seq();
    if (failed || next_response->status() != kOK) {
        LOG(WARNING, "[WriteBlock] WriteNext %s fail: #%ld seq:%d, offset:%ld, len:%lu, "
                     "status= %s, error= %d\n",
            next_server.c_str(), block_id, packet_seq, offset, databuf.size(),
            StatusCode_Name(next_response->status()).c_str(), error);
        if (failed) {
            response->set_status(kNetworkUnavailable);
        } else {
            response->set_status(next_response->status());
        }
        delete next_response;
        g_unfinished_bytes.Sub(databuf.size());
        done->Run();
        return;
    } else {
        LOG(INFO, "[Writeblock] send #%ld seq:%d to next done", block_id, packet_seq);
        delete next_response;
    }

    std::function<void ()> callback =
        std::bind(&ChunkServerImpl::LocalWriteBlock, this, request, response, done);
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
    Block* block = NULL;

    if (packet_seq == 0) {
        StatusCode s;
        block = block_manager_->CreateBlock(block_id, &sync_time, &s);
        if (s != kOK) {
            LOG(INFO, "[LocalWriteBlock] #%ld created failed, reason %s",
                    block_id, StatusCode_Name(s).c_str());
            if (s == kBlockExist) {
                response->set_current_size(block->Size());
                response->set_current_seq(block->GetLastSeq());
                LOG(INFO, "[LocalWriteBlock] #%ld exist block size = %ld", block_id, block->Size());
            }
            response->set_status(s);
            g_unfinished_bytes.Sub(databuf.size());
            done->Run();
            return;
        }
    } else {
        block = block_manager_->FindBlock(block_id);
        if (!block) {
            LOG(WARNING, "[WriteBlock] Block not found: #%ld ", block_id);
            response->set_status(kCsNotFound);
            g_unfinished_bytes.Sub(databuf.size());
            done->Run();
            return;
        }
    }
    if (request->has_recover_version()) {
        block->SetRecover();
    }
    LOG(INFO, "[WriteBlock] local write #%ld %d recover=%d",
        block_id, packet_seq, block->IsRecover());
    int64_t add_used = 0;
    int64_t write_start = common::timer::get_micros();
    if (!block->Write(packet_seq, offset, databuf.data(), databuf.size(), &add_used)) {
        block->DecRef();
        response->set_status(kWriteError);
        g_unfinished_bytes.Sub(databuf.size());
        done->Run();
        return;
    }
    int64_t write_end = common::timer::get_micros();
    if (request->is_last()) {
        if (request->has_recover_version()) {
            block->SetVersion(request->recover_version());
            LOG(INFO, "Recover block set version #%ld V%d ", block_id, request->recover_version());
        }
        block->SetSliceNum(packet_seq + 1);
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
    g_unfinished_bytes.Sub(databuf.size());
    done->Run();
    block->DecRef();
    block = NULL;
}

void ChunkServerImpl::CloseIncompleteBlock(int64_t block_id) {
    LOG(INFO, "[CloseIncompleteBlock] #%ld ", block_id);
    Block* block = block_manager_->FindBlock(block_id);
    if (!block) {
        LOG(INFO, "[CloseIncompleteBlock] Block not found: #%ld ", block_id);
        StatusCode s;
        block = block_manager_->CreateBlock(block_id, NULL, &s);
        if (s != kOK) {
            LOG(WARNING, "[CloseIncompleteBlock] create block fail: #%ld reason: %s",
                block_id, StatusCode_Name(s).c_str());
            return;
        }
        block->Write(0, 0, NULL, 0, NULL);
    }
    block_manager_->CloseBlock(block);
    ReportFinish(block);
    block->DecRef();
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
        std::function<void ()> task =
            std::bind(&ChunkServerImpl::ReadBlock, this, controller, request, response, done);
        read_thread_pool_->AddTask(task);
        return;
    }

    StatusCode status = kOK;

    int64_t find_start = common::timer::get_micros();
    Block* block = block_manager_->FindBlock(block_id);
    if (block == NULL) {
        status = kCsNotFound;
        LOG(WARNING, "ReadBlock not found: #%ld offset: %ld len: %d\n",
                block_id, offset, read_len);
    } else {
        int64_t read_start = common::timer::get_micros();
        char* buf = new char[read_len];
        int64_t len = block->Read(buf, read_len, offset);
        int64_t read_end = common::timer::get_micros();
        if (len >= 0) {
            response->mutable_databuf()->assign(buf, len);
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

void ChunkServerImpl::PushBlock(const ReplicaInfo& new_replica_info, int32_t cancel_time) {
    PushBlockReportRequest report_request;
    report_request.set_sequence_id(0);
    report_request.set_chunkserver_id(chunkserver_id_);
    int64_t block_id = new_replica_info.block_id();
    StatusCode status = PushBlockProcess(new_replica_info, cancel_time);
    report_request.add_blocks(block_id);
    report_request.add_status(status);
    PushBlockReportResponse report_response;
    if (!nameserver_->SendRequest(&NameServer_Stub::PushBlockReport,
                &report_request, &report_response, 15)) {
        LOG(WARNING, "Report push finish fail #%ld ", block_id);
    } else {
        LOG(INFO, "Report push finish done #%ld ", block_id);
    }
    g_recover_count.Dec();
}

StatusCode ChunkServerImpl::PushBlockProcess(const ReplicaInfo& new_replica_info, int32_t cancel_time) {
    int64_t block_id = new_replica_info.block_id();
    Block* block = block_manager_->FindBlock(block_id);
    if (!block) {
        LOG(INFO, "[PushBlock] #%ld failed, does not exist anymore", block_id);
        return kCsNotFound;
    }
    ChunkServer_Stub* chunkserver = NULL;
    int attempts = new_replica_info.chunkserver_address_size();
    StatusCode status = kOK;
    bool timeout = false;
    for (int i = 0; i < attempts; ++i) {
        const std::string& cs_addr = new_replica_info.chunkserver_address(i);
        if (!rpc_client_->GetStub(cs_addr, &chunkserver)) {
            continue;
        }
        LOG(INFO, "[PushBlock] started push #%ld to %s, attempt %d/%d",
                block_id, cs_addr.c_str(), i + 1, attempts);
        status = WriteRecoverBlock(block, chunkserver, cancel_time, &timeout);
        if (status == kOK) {
            LOG(INFO, "[PushBlock] success #%ld to %s", block_id, cs_addr.c_str());
            break;
        } else if (status == kServiceStop) {
            break;
        } else if (timeout) {
            break;
        }
    }
    if (status != kOK) {
        LOG(INFO, "[PushBlock] failed #%ld %s", block_id, StatusCode_Name(status).c_str());
    }
    block->DecRef();
    return status;
}

StatusCode ChunkServerImpl::WriteRecoverBlock(Block* block, ChunkServer_Stub* chunkserver, int32_t cancel_time, bool* timeout) {
    int32_t read_len = 1 << 20;
    int64_t offset = 0;
    int32_t seq = 0;
    char* buf = new char[read_len];
    int64_t start_recover = common::timer::get_micros();
    while (!service_stop_) {
        int32_t now_time = common::timer::now_time();
        if (now_time > cancel_time) {
            delete[] buf;
            *timeout = true;
            return kTimeout;
        }
        int64_t len = block->Read(buf, read_len, offset);
        g_read_bytes.Add(len);
        g_read_ops.Inc();
        if (len < 0) {
            LOG(WARNING, "[WriteRecoverBlock] #%ld read offset %ld len %d return %d",
                    block->Id(), offset, read_len, len);
            delete[] buf;
            return kReadError;
        }
        WriteBlockRequest request;
        WriteBlockResponse response;
        request.set_sequence_id(common::timer::get_micros());
        request.set_block_id(block->Id());
        request.set_databuf(buf, len);
        request.set_is_last(len == 0);
        request.set_packet_seq(seq);
        request.set_offset(offset);
        request.set_recover_version(block->GetVersion());
        bool ret = rpc_client_->SendRequest(chunkserver, &ChunkServer_Stub::WriteBlock,
                                 &request, &response, 60, 1);
        if (!ret || (response.status() != kOK && response.status() != kBlockExist)) {
            LOG(WARNING, "[WriteRecoverBlock] #%ld write failed, offset: %ld len: %ld ret: %d, status: %s",
                    block->Id(), offset, len, ret, StatusCode_Name(response.status()).c_str());
            delete[] buf;
            return kWriteError;
        }
        if (response.status() == kOK) {
            offset += len;
            g_recover_bytes.Add(len);
            ++seq;
        } else if (response.status() == kBlockExist) {
            offset = response.current_size();
            seq = response.current_seq() + 1;
        }
        if (len == 0) {
            delete[] buf;
            int64_t end_recover = common::timer::get_micros();
            LOG(DEBUG, "[WriteRecoverBlock] #%ld finish recover, use %ld ms",
                    block->Id(), (end_recover - start_recover) / 1000);
            return kOK;
        }
    }
    LOG(INFO, "[WriteRecoverBlock] #%ld service_stop_", block->Id());
    delete[] buf;
    return kServiceStop;
}

void ChunkServerImpl::GetBlockInfo(::google::protobuf::RpcController* controller,
                                   const GetBlockInfoRequest* request,
                                   GetBlockInfoResponse* response,
                                   ::google::protobuf::Closure* done) {
    if (!response->has_sequence_id()) {
        response->set_sequence_id(request->sequence_id());
        response->add_timestamp(common::timer::get_micros());
        std::function<void ()> task =
            std::bind(&ChunkServerImpl::GetBlockInfo, this, controller, request, response, done);
        read_thread_pool_->AddTask(task);
        return;
    }
    int64_t block_id = request->block_id();
    StatusCode status = kOK;

    int64_t find_start = common::timer::get_micros();
    Block* block = block_manager_->FindBlock(block_id);
    int64_t find_end = common::timer::get_micros();
    if (block == NULL) {
        status = kCsNotFound;
        LOG(WARNING, "GetBlockInfo not found: #%ld ", block_id);
    } else {
        int64_t block_size = block->GetMeta().block_size();
        response->set_block_size(block_size);
        status = kOK;
        LOG(INFO, "GetBlockInfo #%ld return: %ld "
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
           "<td>Recover(Speed)</td><td>Buffers(new/delete)</td>"
           "<td>PendingTask(W/R/Close/Recv)</td><tr>";
    str += "<tr><td>" + common::NumToString(g_blocks.Get()) + "</td>";
    str += "<td>" + common::HumanReadableString(g_data_size.Get()) + "</td>";
    str += "<td>" + common::NumToString(counters.write_ops) + "</td>";
    str += "<td>" + common::HumanReadableString(counters.write_bytes) + "/S</td>";
    str += "<td>" + common::NumToString(counters.read_ops) + "</td>";
    str += "<td>" + common::HumanReadableString(counters.read_bytes) + "/S</td>";
    str += "<td>" + common::HumanReadableString(counters.recover_bytes) + "/S</td>";
    str += "<td>" + common::NumToString(g_pending_writes.Get()) + "/" +
                    common::NumToString(g_block_buffers.Get()) +
           + "(" + common::NumToString(counters.buffers_new) + "/"
           + common::NumToString(counters.buffers_delete) +")" + "</td>";
    str += "<td>" + common::NumToString(work_thread_pool_->PendingNum()) + "/"
           + common::NumToString(read_thread_pool_->PendingNum()) + "/"
           + common::NumToString(write_thread_pool_->PendingNum()) + "/"
           + common::NumToString(recover_thread_pool_->PendingNum()) + "</td>";
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
    response.content->Append(str);
    return true;
}
} // namespace bfs
} //namespace baidu

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
