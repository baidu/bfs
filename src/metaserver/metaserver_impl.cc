// Copyright (c) 2014, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "metaserver_impl.h"

#include <set>
#include <map>
#include <functional>

#include <gflags/gflags.h>
#include <sofa/pbrpc/pbrpc.h>

#include <common/counter.h>
#include <common/logging.h>
#include <common/string_util.h>

#include "nameserver/block_mapping_manager.h"

#include "nameserver/chunkserver_manager.h"
#include "nameserver/namespace.h"

#include "proto/status_code.pb.h"

DECLARE_int32(metaserver_start_recover_timeout);
DECLARE_int32(chunkserver_max_pending_buffers);
DECLARE_int32(metaserver_report_thread_num);
DECLARE_int32(metaserver_work_thread_num);
DECLARE_int32(metaserver_read_thread_num);
DECLARE_int32(metaserver_heartbeat_thread_num);
DECLARE_int32(blockmapping_bucket_num);
DECLARE_int32(hi_recover_timeout);
DECLARE_int32(lo_recover_timeout);
DECLARE_int32(block_report_timeout);
DECLARE_bool(clean_redundancy);

namespace baidu {
namespace bfs {
namespace metaserver {

common::Counter g_get_location;
common::Counter g_add_block;
common::Counter g_heart_beat;
common::Counter g_block_report;
common::Counter g_unlink;
common::Counter g_create_file;
common::Counter g_list_dir;
common::Counter g_report_blocks;
extern common::Counter g_blocks_num;

MetaServerImpl::MetaServerImpl() : readonly_(true),
    recover_timeout_(0),
    recover_mode_(kStopRecover) {
    block_mapping_manager_ = new BlockMappingManager(FLAGS_blockmapping_bucket_num);
    report_thread_pool_ = new common::ThreadPool(0);
    read_thread_pool_ = new common::ThreadPool(10);
    work_thread_pool_ = new common::ThreadPool(10);
    heartbeat_thread_pool_ = new common::ThreadPool(10);
    chunkserver_manager_ = new ChunkServerManager(work_thread_pool_, block_mapping_manager_);
    start_time_ = common::timer::get_micros();
}

MetaServerImpl::~MetaServerImpl() {
}

void MetaServerImpl::CheckRecoverMode() {
    int now_time = (common::timer::get_micros() - start_time_) / 1000000;
    int recover_timeout = recover_timeout_;
    if (recover_timeout == 0) {
        return;
    }
    int new_recover_timeout =  recover_timeout - now_time;
    if (new_recover_timeout <= 0) {
        LOG(INFO, "Now time %d", now_time);
        recover_mode_ = kRecoverAll;
        return;
    }
    common::atomic_comp_swap(&recover_timeout_, new_recover_timeout, recover_timeout);
    work_thread_pool_->DelayTask(1000, std::bind(&MetaServerImpl::CheckRecoverMode, this));
}
void MetaServerImpl::LeaveReadOnly() {
    LOG(INFO, "Nameserver leave read only");
    if (readonly_) {
        readonly_ = false;
    }
}

void MetaServerImpl::HeartBeat(::google::protobuf::RpcController* controller,
                         const HeartBeatRequest* request,
                         HeartBeatResponse* response,
                         ::google::protobuf::Closure* done) {
    if (!is_leader_) {
        response->set_status(kIsFollower);
        done->Run();
        return;
    }
    g_heart_beat.Inc();
    printf("Receive HeartBeat() from %s\n", request->data_server_addr().c_str());
    int64_t version = request->namespace_version();
    if (version == namespace_->Version()) {
        chunkserver_manager_->HandleHeartBeat(request, response);
    } else {
        response->set_status(kVersionError);
    }
    response->set_namespace_version(namespace_->Version());
    done->Run();
}

void MetaServerImpl::Register(::google::protobuf::RpcController* controller,
                   const ::baidu::bfs::RegisterRequest* request,
                   ::baidu::bfs::RegisterResponse* response,
                   ::google::protobuf::Closure* done) {
    if (!is_leader_) {
        response->set_status(kIsFollower);
        done->Run();
        return;
    }
    sofa::pbrpc::RpcController* sofa_cntl =
        reinterpret_cast<sofa::pbrpc::RpcController*>(controller);
    const std::string& address = request->chunkserver_addr();
    const std::string& ip_address = sofa_cntl->RemoteAddress();
    const std::string cs_ip = ip_address.substr(ip_address.find(':'));
    LOG(INFO, "Register ip: %s", ip_address.c_str());
    int64_t version = request->namespace_version();
    if (version != namespace_->Version()) {
        LOG(INFO, "Register from %s version %ld mismatch %ld, remove internal",
            address.c_str(), version, namespace_->Version());
        chunkserver_manager_->RemoveChunkServer(address);
    } else {
        LOG(INFO, "Register from %s, version= %ld", address.c_str(), version);
        if (chunkserver_manager_->HandleRegister(cs_ip, request, response)) {
            LeaveReadOnly();
        }
    }
    response->set_namespace_version(namespace_->Version());
    done->Run();
}


void MetaServerImpl::BlockReceived(::google::protobuf::RpcController* controller,
                       const BlockReceivedRequest* request,
                       BlockReceivedResponse* response,
                       ::google::protobuf::Closure* done) {
    if (!is_leader_) {
        response->set_status(kIsFollower);
        done->Run();
        return;
    }
    g_block_report.Inc();
    response->set_sequence_id(request->sequence_id());
    int32_t cs_id = request->chunkserver_id();
    LOG(INFO, "BlockReceived from C%d, %s, %d blocks",
        cs_id, request->chunkserver_addr().c_str(), request->blocks_size());
    const ::google::protobuf::RepeatedPtrField<ReportBlockInfo>& blocks = request->blocks();

    common::timer::TimeChecker blockreceived_timer;
    int old_id = chunkserver_manager_->GetChunkServerId(request->chunkserver_addr());
    blockreceived_timer.Check(50 * 1000, "GetChunkServerId");
    if (cs_id != old_id) {
        LOG(INFO, "ChunkServer %s id mismatch, old: C%d new: C%d",
            request->chunkserver_addr().c_str(), old_id, cs_id);
        response->set_status(kUnknownCs);
        done->Run();
        return;
    }
    for (int i = 0; i < blocks.size(); i++) {
        g_report_blocks.Inc();
        const ReportBlockInfo& block =  blocks.Get(i);
        int64_t block_id = block.block_id();
        int64_t block_size = block.block_size();
        int64_t block_version = block.version();
        LOG(INFO, "BlockReceived C%d #%ld V%ld %ld",
            cs_id, block_id, block_version, block_size);
        //update block -> cs;
        blockreceived_timer.Reset();
        if (block_mapping_manager_->UpdateBlockInfo(block_id, cs_id, block_size, block_version)) {
            blockreceived_timer.Check(50 * 1000, "UpdateBlockInfo");
            //update cs -> block
            chunkserver_manager_->AddBlock(cs_id, block_id, block.is_recover());
            blockreceived_timer.Check(50 * 1000, "AddBlock");
        } else {
            LOG(INFO, "BlockReceived drop C%d #%ld V%ld %ld",
                cs_id, block_id, block_version, block_size);
        }
    }
    response->set_status(kOK);
    done->Run();
}

void MetaServerImpl::BlockReport(::google::protobuf::RpcController* controller,
                   const BlockReportRequest* request,
                   BlockReportResponse* response,
                   ::google::protobuf::Closure* done) {
    if (!is_leader_) {
        response->set_status(kIsFollower);
        done->Run();
        return;
    }
    g_block_report.Inc();
    int32_t cs_id = request->chunkserver_id();
    int64_t report_id = request->report_id();
    LOG(INFO, "Report from C%d (%lu) %s %d blocks id %ld start %ld end %ld\n",
        cs_id, request->sequence_id(), request->chunkserver_addr().c_str(),
        request->blocks_size(), report_id, request->start(), request->end());
    const ::google::protobuf::RepeatedPtrField<ReportBlockInfo>& blocks = request->blocks();

    int64_t start_report = common::timer::get_micros();
    int old_id = chunkserver_manager_->GetChunkServerId(request->chunkserver_addr());
    if (cs_id != old_id) {
        LOG(INFO, "ChunkServer %s id mismatch, old: C%d new: C%d , need to re-register",
            request->chunkserver_addr().c_str(), old_id, cs_id);
        response->set_status(kUnknownCs);
        done->Run();
        return;
    }
    int64_t before_update = common::timer::get_micros();
    std::set<int64_t> insert_blocks;
    for (int i = 0; i < blocks.size(); i++) {
        g_report_blocks.Inc();
        const ReportBlockInfo& block =  blocks.Get(i);
        int64_t cur_block_id = block.block_id();
        int64_t cur_block_size = block.block_size();

        // update block -> cs
        int64_t block_version = block.version();
        if (!block_mapping_manager_->UpdateBlockInfo(cur_block_id, cs_id, cur_block_size,
                                                     block_version)) {
            response->add_obsolete_blocks(cur_block_id);
            chunkserver_manager_->RemoveBlock(cs_id, cur_block_id);
            LOG(INFO, "BlockReport remove obsolete block: #%ld C%d ", cur_block_id, cs_id);
            continue;
        } else {
            insert_blocks.insert(cur_block_id);
        }
    }
    int64_t before_add_block = common::timer::get_micros();
    std::vector<int64_t> lost;
    chunkserver_manager_->AddBlockWithCheck(cs_id, insert_blocks, request->start(),
                                   request->end(), &lost, report_id);
    if (lost.size() != 0) {
        LOG(INFO, "C%d lost %u blocks",cs_id, lost.size());
        for (uint32_t i = 0; i < lost.size(); ++i) {
            block_mapping_manager_->DealWithDeadBlock(cs_id, lost[i]);
        }
    }
    int64_t after_add_block = common::timer::get_micros();
    response->set_report_id(report_id);

    // recover replica
    if (recover_mode_ != kStopRecover) {
        RecoverVec recover_blocks;
        int hi_num = 0;
        chunkserver_manager_->PickRecoverBlocks(cs_id, &recover_blocks,
                                                &hi_num, recover_mode_ == kHiOnly);
        int32_t priority = 0;
        for (std::vector<std::pair<int64_t, std::vector<std::string> > >::iterator it =
                recover_blocks.begin(); it != recover_blocks.end(); ++it) {
            ReplicaInfo* rep = response->add_new_replicas();
            rep->set_block_id((*it).first);
            rep->set_priority(priority < hi_num);
            for (std::vector<std::string>::iterator dest_it = (*it).second.begin();
                 dest_it != (*it).second.end(); ++dest_it) {
                rep->add_chunkserver_address(*dest_it);
            }
            rep->set_recover_timeout(priority < hi_num ?
                                     FLAGS_hi_recover_timeout : FLAGS_lo_recover_timeout);
            ++priority;
        }
        LOG(INFO, "Response to C%d %s new_replicas_size= %d",
            cs_id, request->chunkserver_addr().c_str(), response->new_replicas_size());
    }
    block_mapping_manager_->GetCloseBlocks(cs_id, response->mutable_close_blocks());
    int64_t end_report = common::timer::get_micros();
    static __thread int64_t last_warning = 0;
    if (end_report - start_report > 1000 * 1000) {
        int64_t now_time = common::timer::get_micros();
        if (now_time > last_warning + 10 * 1000000) {
            last_warning = now_time;
            LOG(WARNING, "C%d report use %d micors, update use %d micors, add block use %d micors",
                cs_id, end_report - start_report,
                before_add_block - before_update, after_add_block - before_add_block);
        }
    }
    response->set_status(kOK);
    done->Run();
}

void MetaServerImpl::PushBlockReport(::google::protobuf::RpcController* controller,
                   const PushBlockReportRequest* request,
                   PushBlockReportResponse* response,
                   ::google::protobuf::Closure* done) {
    if (!is_leader_) {
        response->set_status(kIsFollower);
        done->Run();
        return;
    }
    response->set_sequence_id(request->sequence_id());
    response->set_status(kOK);
    int32_t cs_id = request->chunkserver_id();
    for (int i = 0; i < request->blocks_size(); i++) {
        block_mapping_manager_->ProcessRecoveredBlock(cs_id, request->blocks(i),
            request->status_size() > i ? request->status(i) : kOK);
    }
    done->Run();
}

void MetaServerImpl::AddBlock(::google::protobuf::RpcController* controller,
                         const AddBlockRequest* request,
                         AddBlockResponse* response,
                         ::google::protobuf::Closure* done) {
    response->set_sequence_id(request->sequence_id());
    if (readonly_) {
        LOG(INFO, "AddBlock for %s failed, safe mode.", request->file_name().c_str());
        response->set_status(kSafeMode);
        done->Run();
        return;
    }
    g_add_block.Inc();
    std::string path = NameSpace::NormalizePath(request->file_name());
    FileInfo file_info;

    if (file_info.blocks_size() > 0) {
        std::map<int64_t, std::set<int32_t> > block_cs;
        block_mapping_manager_->RemoveBlocksForFile(file_info, &block_cs);
        for (std::map<int64_t, std::set<int32_t> >::iterator it = block_cs.begin();
                it != block_cs.end(); ++it) {
            const std::set<int32_t>& cs = it->second;
            for (std::set<int32_t>::iterator cs_it = cs.begin(); cs_it != cs.end(); ++cs_it) {
                chunkserver_manager_->RemoveBlock(*cs_it, it->first);
            }
        }
        file_info.clear_blocks();
    }
    // replica num
    int replica_num = file_info.replicas();
    // check lease for write
    std::vector<std::pair<int32_t, std::string> > chains;
    common::timer::TimeChecker add_block_timer;
    done->Run();
}

 void MetaServerImpl::SyncBlock(::google::protobuf::RpcController* controller,
                                   const SyncBlockRequest* request,
                                   SyncBlockResponse* response,
                                   ::google::protobuf::Closure* done) {
    if (!is_leader_) {
        response->set_status(kIsFollower);
        done->Run();
        return;
    }
    response->set_sequence_id(request->sequence_id());
    FileInfo file_info;
    if (!namespace_->GetFileInfo(file_name, &file_info)) {
        LOG(INFO, "SyncBlock file not found: #%ld %s", block_id, file_name.c_str());
        response->set_status(kNsNotFound);
        done->Run();
        return;
    }
    if (!CheckFileHasBlock(file_info, file_name, block_id)) {
        response->set_status(kNoPermission);
        done->Run();
        return;
    }
    file_info.set_size(request->size());
    MetaServerLog log;
    if (!namespace_->UpdateFileInfo(file_info, &log)) {
        LOG(WARNING, "SyncBlock fail: #%ld %s", block_id, file_name.c_str());
        response->set_status(kUpdateError);
        done->Run();
        return;
    } else {
        LOG(INFO, "SyncBlock #%ld for file %s, V%ld, size: %ld",
                block_id, file_name.c_str(), file_info.version(), file_info.size());
    }
    response->set_status(kOK);
    done->Run();
}

bool MetaServerImpl::CheckFileHasBlock(const FileInfo& file_info,
                                       const std::string& file_name,
                                       int64_t block_id) {
    for (int i = 0; i < file_info.blocks_size(); i++) {
        if (file_info.blocks(i) == block_id) {
            return true;
        }
    }
    LOG(WARNING, "Block #%ld doesn't belog to file %s, ignore it",
        block_id, file_name.c_str());
    return false;
}

void MetaServerImpl::FinishBlock(::google::protobuf::RpcController* controller,
                         const FinishBlockRequest* request,
                         FinishBlockResponse* response,
                         ::google::protobuf::Closure* done) {
    int64_t block_id = request->block_id();
    int64_t block_version = request->block_version();
    response->set_sequence_id(request->sequence_id());
    std::string file_name = NameSpace::NormalizePath(request->file_name());
    sofa::pbrpc::RpcController* ctl = reinterpret_cast<sofa::pbrpc::RpcController*>(controller);
    LOG(INFO, "Sdk %s finish file %s block #%ld",
            ctl->RemoteAddress().c_str(), file_name.c_str(), block_id);
    if (request->close_with_error()) {
        LOG(INFO, "Sdk close %s with error", file_name.c_str());
        block_mapping_manager_->MarkIncomplete(block_id);
        response->set_status(kOK);
        done->Run();
        return;
    }
    FileInfo file_info;
    if (!CheckFileHasBlock(file_info, file_name, block_id)) {
        response->set_status(kNoPermission);
        done->Run();
        return;
    }
    file_info.set_version(block_version);
    file_info.set_size(request->block_size());
    StatusCode ret = block_mapping_manager_->CheckBlockVersion(block_id, block_version);
    response->set_status(ret);
    if (ret != kOK) {
        LOG(INFO, "FinishBlock fail: #%ld %s", block_id, file_name.c_str());
    } else {
        LOG(INFO, "FinishBlock #%ld %s", block_id, file_name.c_str());
    }
    done->Run();
}

}  //namespace metaserver
}  //namespace bfs
}  //namespace baidu

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
