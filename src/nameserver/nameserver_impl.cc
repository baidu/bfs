// Copyright (c) 2014, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#include "nameserver_impl.h"

#include <set>
#include <map>
#include <sstream>
#include <cstdlib>

#include <gflags/gflags.h>
#include <sofa/pbrpc/pbrpc.h>

#include <common/counter.h>
#include <common/logging.h>
#include <common/string_util.h>

#include "nameserver/block_mapping_manager.h"

#include "nameserver/sync.h"
#include "nameserver/chunkserver_manager.h"
#include "nameserver/namespace.h"

#include "proto/status_code.pb.h"

DECLARE_bool(bfs_web_kick_enable);
DECLARE_int32(nameserver_start_recover_timeout);
DECLARE_int32(chunkserver_max_pending_buffers);
DECLARE_int32(nameserver_report_thread_num);
DECLARE_int32(nameserver_work_thread_num);
DECLARE_int32(nameserver_read_thread_num);
DECLARE_int32(nameserver_heartbeat_thread_num);
DECLARE_int32(blockmapping_bucket_num);
DECLARE_int32(hi_recover_timeout);
DECLARE_int32(lo_recover_timeout);
DECLARE_int32(block_report_timeout);
DECLARE_bool(clean_redundancy);

namespace baidu {
namespace bfs {

common::Counter g_get_location;
common::Counter g_add_block;
common::Counter g_heart_beat;
common::Counter g_block_report;
common::Counter g_unlink;
common::Counter g_create_file;
common::Counter g_list_dir;
common::Counter g_report_blocks;
extern common::Counter g_blocks_num;

NameServerImpl::NameServerImpl(Sync* sync) : readonly_(true),
    recover_timeout_(FLAGS_nameserver_start_recover_timeout),
    recover_mode_(kStopRecover), sync_(sync) {
    block_mapping_manager_ = new BlockMappingManager(FLAGS_blockmapping_bucket_num);
    report_thread_pool_ = new common::ThreadPool(FLAGS_nameserver_report_thread_num);
    read_thread_pool_ = new common::ThreadPool(FLAGS_nameserver_read_thread_num);
    work_thread_pool_ = new common::ThreadPool(FLAGS_nameserver_work_thread_num);
    heartbeat_thread_pool_ = new common::ThreadPool(FLAGS_nameserver_heartbeat_thread_num);
    chunkserver_manager_ = new ChunkServerManager(work_thread_pool_, block_mapping_manager_);
    namespace_ = new NameSpace(false);
    if (sync_) {
        sync_->Init(std::bind(&NameSpace::TailLog, namespace_, std::placeholders::_1));
    }
    CheckLeader();
    start_time_ = common::timer::get_micros();
    read_thread_pool_->AddTask(std::bind(&NameServerImpl::LogStatus, this));
}

NameServerImpl::~NameServerImpl() {
}

void NameServerImpl::CheckLeader() {
    if (!sync_ || sync_->IsLeader()) {
        LOG(INFO, "Leader nameserver, rebuild block map.");
        NameServerLog log;
        std::function<void (const FileInfo&)> task =
            std::bind(&NameServerImpl::RebuildBlockMapCallback, this, std::placeholders::_1);
        namespace_->Activate(task, &log);
        if (!LogRemote(log, std::function<void (bool)>())) {
            LOG(FATAL, "LogRemote namespace update fail");
        }
        recover_timeout_ = FLAGS_nameserver_start_recover_timeout;
        start_time_ = common::timer::get_micros();
        work_thread_pool_->DelayTask(1000, std::bind(&NameServerImpl::CheckRecoverMode, this));
        is_leader_ = true;
    } else {
        is_leader_ = false;
        work_thread_pool_->DelayTask(100, std::bind(&NameServerImpl::CheckLeader, this));
        //LOG(INFO, "Delay CheckLeader");
    }
}

void NameServerImpl::CheckRecoverMode() {
    int now_time = (common::timer::get_micros() - start_time_) / 1000000;
    int recover_timeout = recover_timeout_;
    if (recover_timeout == 0) {
        return;
    }
    int new_recover_timeout = FLAGS_nameserver_start_recover_timeout - now_time;
    if (new_recover_timeout <= 0) {
        LOG(INFO, "Now time %d", now_time);
        recover_mode_ = kRecoverAll;
        return;
    }
    common::atomic_comp_swap(&recover_timeout_, new_recover_timeout, recover_timeout);
    work_thread_pool_->DelayTask(1000, std::bind(&NameServerImpl::CheckRecoverMode, this));
}
void NameServerImpl::LeaveReadOnly() {
    LOG(INFO, "Nameserver leave read only");
    if (readonly_) {
        readonly_ = false;
    }
}

void NameServerImpl::LogStatus() {
    LOG(INFO, "[Status] create %ld list %ld get_loc %ld add_block %ld "
              "unlink %ld report %ld %ld heartbeat %ld read_pending %ld "
              "work_pending %ld report_pending %ld",
        g_create_file.Clear(), g_list_dir.Clear(), g_get_location.Clear(),
        g_add_block.Clear(), g_unlink.Clear(), g_block_report.Clear(),
        g_report_blocks.Clear(), g_heart_beat.Clear(),
        read_thread_pool_->PendingNum(),
        work_thread_pool_->PendingNum(), report_thread_pool_->PendingNum());
    work_thread_pool_->DelayTask(1000, std::bind(&NameServerImpl::LogStatus, this));
}

void NameServerImpl::HeartBeat(::google::protobuf::RpcController* controller,
                         const HeartBeatRequest* request,
                         HeartBeatResponse* response,
                         ::google::protobuf::Closure* done) {
    if (!is_leader_) {
        response->set_status(kIsFollower);
        done->Run();
        return;
    }
    g_heart_beat.Inc();
    // printf("Receive HeartBeat() from %s\n", request->data_server_addr().c_str());
    int64_t version = request->namespace_version();
    if (version == namespace_->Version()) {
        chunkserver_manager_->HandleHeartBeat(request, response);
    } else {
        response->set_status(kVersionError);
    }
    response->set_namespace_version(namespace_->Version());
    done->Run();
}

void NameServerImpl::Register(::google::protobuf::RpcController* controller,
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


void NameServerImpl::BlockReceived(::google::protobuf::RpcController* controller,
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
        // update block -> cs;
        blockreceived_timer.Reset();
        if (block_mapping_manager_->UpdateBlockInfo(block_id, cs_id, block_size, block_version)) {
            blockreceived_timer.Check(50 * 1000, "UpdateBlockInfo");
            // update cs -> block
            chunkserver_manager_->AddBlock(cs_id, block_id);
            blockreceived_timer.Check(50 * 1000, "AddBlock");
        } else {
            LOG(INFO, "BlockReceived drop C%d #%ld V%ld %ld",
                cs_id, block_id, block_version, block_size);
        }
    }
    response->set_status(kOK);
    done->Run();
}

void NameServerImpl::BlockReport(::google::protobuf::RpcController* controller,
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
    int64_t ret = chunkserver_manager_->AddBlockWithCheck(cs_id, insert_blocks, request->start(),
                                   request->end(), &lost, report_id);
    if (lost.size() != 0) {
        LOG(INFO, "C%d lost %u blocks",cs_id, lost.size());
        for (uint32_t i = 0; i < lost.size(); ++i) {
            block_mapping_manager_->DealWithDeadBlock(cs_id, lost[i]);
        }
    }
    int64_t after_add_block = common::timer::get_micros();
    response->set_report_id(ret);

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

void NameServerImpl::PushBlockReport(::google::protobuf::RpcController* controller,
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

void NameServerImpl::CreateFile(::google::protobuf::RpcController* controller,
                        const CreateFileRequest* request,
                        CreateFileResponse* response,
                        ::google::protobuf::Closure* done) {
    if (!is_leader_) {
        response->set_status(kIsFollower);
        done->Run();
        return;
    }
    g_create_file.Inc();
    response->set_sequence_id(request->sequence_id());
    std::string path = NameSpace::NormalizePath(request->file_name());
    int flags = request->flags();
    int mode = request->mode();
    if (mode == 0) {
        mode = 0644;    // default mode
    }
    int replica_num = request->replica_num();
    NameServerLog log;
    std::vector<int64_t> blocks_to_remove;
    StatusCode status = namespace_->CreateFile(path, flags, mode, replica_num, &blocks_to_remove, &log);
    for (size_t i = 0; i < blocks_to_remove.size(); i++) {
        block_mapping_manager_->RemoveBlock(blocks_to_remove[i]);
    }
    response->set_status(status);
    sofa::pbrpc::RpcController* ctl = reinterpret_cast<sofa::pbrpc::RpcController*>(controller);
    LOG(INFO, "Sdk %s create file %s returns %s",
            ctl->RemoteAddress().c_str(), path.c_str(), StatusCode_Name(status).c_str());
    if (status != kOK) {
        done->Run();
        return;
    }
    LogRemote(log, std::bind(&NameServerImpl::SyncLogCallback, this,
                               controller, request, response, done,
                               (std::vector<FileInfo>*)NULL, std::placeholders::_1));
}

bool NameServerImpl::LogRemote(const NameServerLog& log, std::function<void (bool)> callback) {
    if (sync_ == NULL) {
        if (callback) {
            work_thread_pool_->AddTask(std::bind(callback, true));
        }
        return true;
    }
    std::string logstr;
    if (!log.SerializeToString(&logstr)) {
        LOG(FATAL, "Serialize log fail");
    }
    if (callback) {
        sync_->Log(logstr, callback);
        return true;
    } else {
        return sync_->Log(logstr);
    }
}

void NameServerImpl::SyncLogCallback(::google::protobuf::RpcController* controller,
                                     const ::google::protobuf::Message* request,
                                     ::google::protobuf::Message* response,
                                     ::google::protobuf::Closure* done,
                                     std::vector<FileInfo>* removed,
                                     bool ret) {
    if (!ret) {
        controller->SetFailed("SyncLogFail");
    } else if (removed) {
        for (uint32_t i = 0; i < removed->size(); i++) {
            block_mapping_manager_->RemoveBlocksForFile((*removed)[i], NULL);
        }
        delete removed;
    }
    done->Run();
    if (!ret) {
        LOG(FATAL, "SyncLog fail");
    }
}

void NameServerImpl::AddBlock(::google::protobuf::RpcController* controller,
                         const AddBlockRequest* request,
                         AddBlockResponse* response,
                         ::google::protobuf::Closure* done) {
    if (!is_leader_) {
        response->set_status(kIsFollower);
        done->Run();
        return;
    }
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
    if (!namespace_->GetFileInfo(path, &file_info)) {
        LOG(INFO, "AddBlock file not found: %s", path.c_str());
        response->set_status(kNsNotFound);
        done->Run();
        return;
    }

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
    /// replica num
    int replica_num = file_info.replicas();
    /// check lease for write
    std::vector<std::pair<int32_t, std::string> > chains;
    common::timer::TimeChecker add_block_timer;
    if (chunkserver_manager_->GetChunkServerChains(replica_num, &chains, request->client_address())) {
        add_block_timer.Check(50 * 1000, "GetChunkServerChains");
        NameServerLog log;
        int64_t new_block_id = namespace_->GetNewBlockId(&log);
        LOG(INFO, "[AddBlock] new block for %s #%ld R%d %s",
            path.c_str(), new_block_id, replica_num, request->client_address().c_str());
        file_info.add_blocks(new_block_id);
        file_info.set_version(-1);
        ///TODO: Lost update? Get&Update not atomic.
        for (int i = 0; i < replica_num; i++) {
            file_info.add_cs_addrs(chunkserver_manager_->GetChunkServerAddr(chains[i].first));
        }
        if (!namespace_->UpdateFileInfo(file_info, &log)) {
            LOG(WARNING, "Update file info fail: %s", path.c_str());
            response->set_status(kUpdateError);
        }
        LocatedBlock* block = response->mutable_block();
        std::vector<int32_t> replicas;
        for (int i = 0; i < replica_num; i++) {
            ChunkServerInfo* info = block->add_chains();
            int32_t cs_id = chains[i].first;
            info->set_address(chains[i].second);
            LOG(INFO, "Add C%d %s to #%ld response",
                cs_id, chains[i].second.c_str(), new_block_id);
            replicas.push_back(cs_id);
            // update cs -> block
            add_block_timer.Reset();
            chunkserver_manager_->AddBlock(cs_id, new_block_id);
            add_block_timer.Check(50 * 1000, "AddBlock");
        }
        block_mapping_manager_->AddNewBlock(new_block_id, replica_num, -1, 0, &replicas);
        add_block_timer.Check(50 * 1000, "AddNewBlock");
        block->set_block_id(new_block_id);
        response->set_status(kOK);
        LogRemote(log, std::bind(&NameServerImpl::SyncLogCallback, this,
                                   controller, request, response, done,
                                   (std::vector<FileInfo>*)NULL, std::placeholders::_1));
    } else {
        LOG(WARNING, "AddBlock for %s failed.", path.c_str());
        response->set_status(kGetChunkServerError);
        done->Run();
    }
}

void NameServerImpl::SyncBlock(::google::protobuf::RpcController* controller,
                                   const SyncBlockRequest* request,
                                   SyncBlockResponse* response,
                                   ::google::protobuf::Closure* done) {
    if (!is_leader_) {
        response->set_status(kIsFollower);
        done->Run();
        return;
    }
    int64_t block_id = request->block_id();
    response->set_sequence_id(request->sequence_id());
    std::string file_name = NameSpace::NormalizePath(request->file_name());
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
    NameServerLog log;
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
    LogRemote(log, std::bind(&NameServerImpl::SyncLogCallback, this,
                               controller, request, response, done,
                               (std::vector<FileInfo>*)NULL, std::placeholders::_1));
}

bool NameServerImpl::CheckFileHasBlock(const FileInfo& file_info,
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

void NameServerImpl::FinishBlock(::google::protobuf::RpcController* controller,
                         const FinishBlockRequest* request,
                         FinishBlockResponse* response,
                         ::google::protobuf::Closure* done) {
    if (!is_leader_) {
        response->set_status(kIsFollower);
        done->Run();
        return;
    }
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
    if (!namespace_->GetFileInfo(file_name, &file_info)) {
        LOG(INFO, "FinishBlock file not found: #%ld %s", block_id, file_name.c_str());
        response->set_status(kNsNotFound);
        done->Run();
        return;
    }

    if (!CheckFileHasBlock(file_info, file_name, block_id)) {
        response->set_status(kNoPermission);
        done->Run();
        return;
    }
    file_info.set_version(block_version);
    file_info.set_size(request->block_size());
    NameServerLog log;
    if (!namespace_->UpdateFileInfo(file_info, &log)) {
        LOG(WARNING, "FinishBlock fail: #%ld %s", block_id, file_name.c_str());
        response->set_status(kUpdateError);
        done->Run();
        return;
    }
    StatusCode ret = block_mapping_manager_->CheckBlockVersion(block_id, block_version);
    response->set_status(ret);
    if (ret != kOK) {
        LOG(INFO, "FinishBlock fail: #%ld %s", block_id, file_name.c_str());
        done->Run();
    } else {
        LOG(INFO, "FinishBlock #%ld %s", block_id, file_name.c_str());
        LogRemote(log, std::bind(&NameServerImpl::SyncLogCallback, this,
                                   controller, request, response, done,
                                   (std::vector<FileInfo>*)NULL, std::placeholders::_1));
    }
}

void NameServerImpl::GetFileLocation(::google::protobuf::RpcController* controller,
                      const FileLocationRequest* request,
                      FileLocationResponse* response,
                      ::google::protobuf::Closure* done) {
    if (!is_leader_) {
        response->set_status(kIsFollower);
        done->Run();
        return;
    }
    response->set_sequence_id(request->sequence_id());
    std::string path = NameSpace::NormalizePath(request->file_name());
    LOG(INFO, "NameServerImpl::GetFileLocation: %s\n", request->file_name().c_str());
    // Get file_key
    g_get_location.Inc();

    FileInfo info;
    if (!namespace_->GetFileInfo(path, &info)) {
        // No this file
        LOG(INFO, "NameServerImpl::GetFileLocation: NotFound: %s",
            request->file_name().c_str());
        response->set_status(kNsNotFound);
    } else {
        for (int i = 0; i < info.blocks_size(); i++) {
            int64_t block_id = info.blocks(i);
            std::vector<int32_t> replica;
            int64_t block_size = 0;
            RecoverStat rs;
            if (!block_mapping_manager_->GetLocatedBlock(block_id, &replica, &block_size, &rs)) {
                LOG(WARNING, "GetFileLocation GetBlockReplica fail #%ld ", block_id);
                break;
            }
            LocatedBlock* lcblock = response->add_blocks();
            lcblock->set_block_id(block_id);
            lcblock->set_block_size(block_size);
            lcblock->set_status(rs);
            for (uint32_t i = 0; i < replica.size(); i++) {
                int32_t server_id = replica[i];
                std::string addr = chunkserver_manager_->GetChunkServerAddr(server_id);
                if (addr == "") {
                    LOG(WARNING, "GetChunkServerAddr from id: C%d fail.", server_id);
                    continue;
                }
                LOG(INFO, "return server C%d %s for #%ld ", server_id, addr.c_str(), block_id);
                ChunkServerInfo* cs_info = lcblock->add_chains();
                cs_info->set_address(addr);
            }
            LOG(INFO, "NameServerImpl::GetFileLocation: %s return #%ld R%lu",
                request->file_name().c_str(), block_id, replica.size());
        }
        // success if file exist
        response->set_status(kOK);
    }
    done->Run();
}

void NameServerImpl::ListDirectory(::google::protobuf::RpcController* controller,
                        const ListDirectoryRequest* request,
                        ListDirectoryResponse* response,
                        ::google::protobuf::Closure* done) {
    if (!is_leader_) {
        response->set_status(kIsFollower);
        done->Run();
        return;
    }
    g_list_dir.Inc();
    response->set_sequence_id(request->sequence_id());
    std::string path = NameSpace::NormalizePath(request->path());
    common::timer::AutoTimer at(100, "ListDirectory", path.c_str());

    StatusCode status = namespace_->ListDirectory(path, response->mutable_files());
    response->set_status(status);
    done->Run();
}

void NameServerImpl::Stat(::google::protobuf::RpcController* controller,
                          const StatRequest* request,
                          StatResponse* response,
                          ::google::protobuf::Closure* done) {
    if (!is_leader_) {
        response->set_status(kIsFollower);
        done->Run();
        return;
    }
    response->set_sequence_id(request->sequence_id());
    std::string path = NameSpace::NormalizePath(request->path());
    LOG(INFO, "Stat: %s\n", path.c_str());

    FileInfo info;
    if (namespace_->GetFileInfo(path, &info)) {
        FileInfo* out_info = response->mutable_file_info();
        out_info->CopyFrom(info);
        //maybe haven't been written info meta
        if (!out_info->size()) {
            int64_t file_size = 0;
            for (int i = 0; i < out_info->blocks_size(); i++) {
                int64_t block_id = out_info->blocks(i);
                NSBlock nsblock;
                if (!block_mapping_manager_->GetBlock(block_id, &nsblock)) {
                    continue;
                }
                file_size += nsblock.block_size;
            }
            out_info->set_size(file_size);
        }
        response->set_status(kOK);
        LOG(INFO, "Stat: %s return: %ld", path.c_str(), out_info->size());
    } else {
        LOG(INFO, "Stat: %s return: not found", path.c_str());
        response->set_status(kNsNotFound);
    }
    done->Run();
}

void NameServerImpl::Rename(::google::protobuf::RpcController* controller,
                            const RenameRequest* request,
                            RenameResponse* response,
                            ::google::protobuf::Closure* done) {
    if (!is_leader_) {
        response->set_status(kIsFollower);
        done->Run();
        return;
    }
    response->set_sequence_id(request->sequence_id());
    std::string oldpath = NameSpace::NormalizePath(request->oldpath());
    std::string newpath = NameSpace::NormalizePath(request->newpath());

    bool need_unlink;
    FileInfo remove_file;
    NameServerLog log;
    StatusCode status = namespace_->Rename(oldpath, newpath, &need_unlink, &remove_file, &log);
    response->set_status(status);
    if (status != kOK) {
        done->Run();
        return;
    }
    std::vector<FileInfo>* removed = NULL;
    if (need_unlink) {
        removed = new std::vector<FileInfo>;
        removed->push_back(remove_file);
    }
    LogRemote(log, std::bind(&NameServerImpl::SyncLogCallback, this,
                               controller, request, response, done, removed, std::placeholders::_1));
}

void NameServerImpl::Unlink(::google::protobuf::RpcController* controller,
                            const UnlinkRequest* request,
                            UnlinkResponse* response,
                            ::google::protobuf::Closure* done) {
    if (!is_leader_) {
        response->set_status(kIsFollower);
        done->Run();
        return;
    }
    g_unlink.Inc();
    response->set_sequence_id(request->sequence_id());
    std::string path = NameSpace::NormalizePath(request->path());

    FileInfo file_info;
    NameServerLog log;
    StatusCode status = namespace_->RemoveFile(path, &file_info, &log);
    sofa::pbrpc::RpcController* ctl = reinterpret_cast<sofa::pbrpc::RpcController*>(controller);
    LOG(INFO, "Sdk %s unlink file %s returns %s",
            ctl->RemoteAddress().c_str(), path.c_str(), StatusCode_Name(status).c_str());
    response->set_status(status);
    if (status != kOK) {
        done->Run();
        return;
    }
    std::vector<FileInfo>* removed = new std::vector<FileInfo>;
    removed->push_back(file_info);
    LogRemote(log, std::bind(&NameServerImpl::SyncLogCallback, this,
                               controller, request, response, done,
                               removed, std::placeholders::_1));
}

void NameServerImpl::DiskUsage(::google::protobuf::RpcController* controller,
                               const DiskUsageRequest* request,
                               DiskUsageResponse* response,
                               ::google::protobuf::Closure* done) {
    if (!is_leader_) {
        response->set_status(kIsFollower);
        done->Run();
        return;
    }
    response->set_sequence_id(request->sequence_id());
    std::string path = NameSpace::NormalizePath(request->path());
    if (path.empty() || path[0] != '/') {
        response->set_status(kBadParameter);
        done->Run();
        return;
    }
    uint64_t du_size = 0;
    StatusCode ret_status = namespace_->DiskUsage(path, &du_size);
    response->set_status(ret_status);
    response->set_du_size(du_size);
    done->Run();
    return;
}

void NameServerImpl::DeleteDirectory(::google::protobuf::RpcController* controller,
                                     const DeleteDirectoryRequest* request,
                                     DeleteDirectoryResponse* response,
                                     ::google::protobuf::Closure* done)  {
    if (!is_leader_) {
        response->set_status(kIsFollower);
        done->Run();
        return;
    }
    response->set_sequence_id(request->sequence_id());
    std::string path = NameSpace::NormalizePath(request->path());
    bool recursive = request->recursive();
    if (path.empty() || path[0] != '/') {
        response->set_status(kBadParameter);
        done->Run();
        return;
    }
    std::vector<FileInfo>* removed = new std::vector<FileInfo>;
    NameServerLog log;
    StatusCode ret_status = namespace_->DeleteDirectory(path, recursive, removed, &log);
    sofa::pbrpc::RpcController* ctl = reinterpret_cast<sofa::pbrpc::RpcController*>(controller);
    LOG(INFO, "Sdk %s delete directory %s returns %s",
            ctl->RemoteAddress().c_str(), path.c_str(), StatusCode_Name(ret_status).c_str());
    response->set_status(ret_status);
    if (ret_status != kOK) {
        done->Run();
        return;
    }
    LogRemote(log, std::bind(&NameServerImpl::SyncLogCallback, this,
                               controller, request, response, done, removed, std::placeholders::_1));
}

void NameServerImpl::ChangeReplicaNum(::google::protobuf::RpcController* controller,
                                      const ChangeReplicaNumRequest* request,
                                      ChangeReplicaNumResponse* response,
                                      ::google::protobuf::Closure* done) {
    if (!is_leader_) {
        response->set_status(kIsFollower);
        done->Run();
        return;
    }
    response->set_sequence_id(request->sequence_id());
    std::string file_name = NameSpace::NormalizePath(request->file_name());
    int32_t replica_num = request->replica_num();
    StatusCode ret_status = kOK;
    FileInfo file_info;
    if (namespace_->GetFileInfo(file_name, &file_info)) {
        file_info.set_replicas(replica_num);
        NameServerLog log;
        bool ret = namespace_->UpdateFileInfo(file_info, &log);
        assert(ret);
        for (int i = 0; i < file_info.blocks_size(); i++) {
            if (block_mapping_manager_->ChangeReplicaNum(file_info.blocks(i), replica_num)) {
                LOG(INFO, "Change %s replica num to %d", file_name.c_str(), replica_num);
            } else {
                ///TODO: need to undo when file have multiple blocks?
                LOG(WARNING, "Change %s replica num to %d fail", file_name.c_str(), replica_num);
                ret_status = kNotOK;
                break;
            }
        }
        response->set_status(kOK);
        LogRemote(log, std::bind(&NameServerImpl::SyncLogCallback, this,
                                   controller, request, response, done,
                                   (std::vector<FileInfo>*)NULL, std::placeholders::_1));
        return;
    } else {
        LOG(INFO, "Change replica num not found: %s", file_name.c_str());
        ret_status = kNsNotFound;
    }
    response->set_status(ret_status);
    done->Run();
}

void NameServerImpl::RebuildBlockMapCallback(const FileInfo& file_info) {
    for (int i = 0; i < file_info.blocks_size(); i++) {
        int64_t block_id = file_info.blocks(i);
        int64_t version = file_info.version();
        block_mapping_manager_->AddNewBlock(block_id, file_info.replicas(),
                                    version, file_info.size(), NULL);
    }
}

void NameServerImpl::SysStat(::google::protobuf::RpcController* controller,
                             const SysStatRequest* request,
                             SysStatResponse* response,
                             ::google::protobuf::Closure* done) {
    if (!is_leader_) {
        response->set_status(kIsFollower);
        done->Run();
        return;
    }
    sofa::pbrpc::RpcController* ctl = reinterpret_cast<sofa::pbrpc::RpcController*>(controller);
    LOG(INFO, "SysStat from %s", ctl->RemoteAddress().c_str());
    chunkserver_manager_->ListChunkServers(response->mutable_chunkservers());
    response->set_status(kOK);
    done->Run();
}

void NameServerImpl::ShutdownChunkServer(::google::protobuf::RpcController* controller,
        const ShutdownChunkServerRequest* request,
        ShutdownChunkServerResponse* response,
        ::google::protobuf::Closure* done) {
    if (!is_leader_) {
        response->set_status(kIsFollower);
        done->Run();
        return;
    }
    StatusCode status = chunkserver_manager_->ShutdownChunkServer(request->chunkserver_address());
    response->set_status(status);
    done->Run();
}

void NameServerImpl::ShutdownChunkServerStat(::google::protobuf::RpcController* controller,
        const ShutdownChunkServerStatRequest* request,
        ShutdownChunkServerStatResponse* response,
        ::google::protobuf::Closure* done) {
    if (!is_leader_) {
        response->set_status(kIsFollower);
        done->Run();
        return;
    }
    bool in_progress = chunkserver_manager_->GetShutdownChunkServerStat();
    response->set_status(kOK);
    response->set_in_offline_progress(in_progress);
    done->Run();
}

void NameServerImpl::TransToString(const std::map<int32_t, std::set<int64_t> >& chk_set,
                                    std::string* output) {
    for (std::map<int32_t, std::set<int64_t> >::const_iterator it =
            chk_set.begin(); it != chk_set.end(); ++it) {
        output->append(common::NumToString(it->first) + ": ");
        const std::set<int64_t>& block_set = it->second;
        std::string cur_cs_str;
        TransToString(block_set, &cur_cs_str);
        output->append(cur_cs_str);
        output->append("<br>");
    }
}

void NameServerImpl::TransToString(const std::set<int64_t>& block_set, std::string* output) {
    for (std::set<int64_t>::const_iterator it = block_set.begin();
            it != block_set.end(); ++it) {
        output->append(common::NumToString(*it) + " ");
    }
}


void NameServerImpl::ListRecover(sofa::pbrpc::HTTPResponse* response) {
    RecoverBlockSet recover_blocks;
    block_mapping_manager_->ListRecover(&recover_blocks);
    std::string hi_recover, lo_recover, lost, hi_check, lo_check, incomplete;
    TransToString(recover_blocks.hi_recover, &hi_recover);
    TransToString(recover_blocks.lo_recover, &lo_recover);
    TransToString(recover_blocks.lost, &lost);
    TransToString(recover_blocks.hi_check, &hi_check);
    TransToString(recover_blocks.lo_check, &lo_check);
    TransToString(recover_blocks.incomplete, &incomplete);
    std::string str =
            "<html><head><title>Recover Details</title>\n"
            "<meta http-equiv=\"Content-Type\" content=\"text/html; charset=utf-8\" />\n"
            "<script src=\"http://libs.baidu.com/jquery/1.8.3/jquery.min.js\"></script>\n"
            "<link href=\"http://apps.bdimg.com/libs/bootstrap/3.2.0/css/bootstrap.min.css\" "
                "rel=\"stylesheet\">\n"
            "</head>\n";
    str += "<body><div class=\"col-sm-12  col-md-12\">";
    str += "<h1>分布式文件系统控制台 - RecoverDetails</h1>";

    str += "<table class=\"table\">";
    str += "<tr><td>lost</td></tr>";
    str += "<tr><td>" + lost + "</td></tr>";
    str += "<tr><td>incomplete</td></tr>";
    str += "<tr><td>" + incomplete + "</td></tr>";
    str += "<tr><td>hi_check</td></tr>";
    str += "<tr><td>" + hi_check + "</td></tr>";
    str += "<tr><td>lo_check</td></tr>";
    str += "<tr><td>" + lo_check + "</td></tr>";
    str += "<tr><td>hi_recover</td></tr>";
    str += "<tr><td>" + hi_recover + "</td></tr>";
    str += "<tr><td>lo_recover</td></tr>";
    str += "<tr><td>" + lo_recover + "</td></tr></table>";

    str += "</div></body><html>";
    response->content->Append(str);
    return;
}

bool NameServerImpl::WebService(const sofa::pbrpc::HTTPRequest& request,
                                sofa::pbrpc::HTTPResponse& response) {
    const std::string& path = request.path;
    DisplayMode display_mode = kDisplayAll;
    if (path == "/dfs/switchtoleader") {
        if (sync_) {
            sync_->SwitchToLeader();
        }
        return true;
    } else if (path == "/dfs/details") {
        ListRecover(&response);
        return true;
    } else if (path == "/dfs/hi_only") {
        recover_timeout_ = 0;
        LOG(INFO, "ChangeRecoverMode hi_only");
        recover_mode_ = kHiOnly;
        response.content->Append("<body onload=\"history.back()\"></body>");
        return true;
    } else if (path == "/dfs/recover_all") {
        recover_timeout_ = 0;
        LOG(INFO, "ChangeRecoverMode recover_all");
        recover_mode_ = kRecoverAll;
        response.content->Append("<body onload=\"history.back()\"></body>");
        return true;
    } else if (path == "/dfs/stop_recover") {
        recover_timeout_ = 0;
        LOG(INFO, "ChangeRecoverMode stop_recover");
        recover_mode_ = kStopRecover;
        response.content->Append("<body onload=\"history.back()\"></body>");
        return true;
    } else if (path == "/dfs/leave_read_only") {
        LOG(INFO, "ChangeStatus leave_read_only");
        LeaveReadOnly();
        response.content->Append("<body onload=\"history.back()\"></body>");
        return true;
    } else if (path == "/dfs/entry_read_only") {
        LOG(INFO, "ChangeStatus entry_read_only");
        readonly_ = true;
        response.content->Append("<body onload=\"history.back()\"></body>");
        return true;
    } else if (path == "/dfs/kick" && FLAGS_bfs_web_kick_enable) {
        std::map<std::string, std::string>::const_iterator it =
            request.query_params->find("cs");
        if (it == request.query_params->end()) {
            return false;
        }
        std::stringstream ss(it->second);
        int cs_id;
        if (ss >> cs_id && chunkserver_manager_->KickChunkServer(cs_id)) {
            response.content->Append("<body onload=\"history.back()\"></body>");
            return true;
        }
        return false;
    } else if (path == "/dfs/alive") {
        display_mode = kAliveOnly;
    } else if (path == "/dfs/dead") {
        display_mode = kDeadOnly;
    } else if (path == "/dfs/overload") {
        display_mode = kOverload;
    } else if (path == "/dfs/set") {
        std::map<const std::string, std::string>::const_iterator it = request.query_params->begin();
        Params p;
        if (it != request.query_params->end()) {
            int32_t v = 0;
            if (it->first != "clean_redundancy") {
                v = std::atoi(it->second.c_str());
            }
            if (it->first == "report_interval") {
                if (v < 1 || v > 3600) {
                    response.content->Append("<h1>Bad Parameter : 1 <= report_interval <= 3600 </h1>");
                    return true;
                }
                p.set_report_interval(v);
            } else if (it->first == "report_size") {
                if (v < 1 || v > 200000) {
                    response.content->Append("<h1>Bad Parameter : 1 <= report_size <= 200000 </h1>");
                    return true;
                }
                p.set_report_size(v);
            } else if (it->first == "recover_size") {
                if (v < 1 || v > 250) {
                    response.content->Append("<h1>Bad Parameter : 1 <= recover_size <= 250 </h1>");
                    return true;
                }
                p.set_recover_size(v);
            } else if (it->first == "keepalive_timeout") {
                if (v < 2 || v > 3600) {
                    response.content->Append("<h1>Bad Parameter : 2 <= keepalive_timeout <= 3600 </h1>");
                    return true;
                }
                p.set_keepalive_timeout(v);
            } else if (it->first == "block_report_timeout") {
                if (v < 2 || v > 3600) {
                    response.content->Append("<h1>Bad Parameter : 2 <= block_report_timeout <= 3600 </h1>");
                    return true;
                }
                FLAGS_block_report_timeout = v;
            } else if (it->first == "clean_redundancy") {
                if (it->second != "true" && it->second != "false") {
                    response.content->Append("<h1>Bad Parameter : clean_redundancy == true || false");
                    return true;
                }
                FLAGS_clean_redundancy = it->second == "true" ? true : false;
            } else {
                response.content->Append("<h1>Bad Parameter :");
                response.content->Append(it->first);
                return true;
            }
            chunkserver_manager_->SetParam(p);
            response.content->Append("<body onload=\"history.back()\"></body>");
            return true;
        }
    }

    ::google::protobuf::RepeatedPtrField<ChunkServerInfo>* chunkservers
        = new ::google::protobuf::RepeatedPtrField<ChunkServerInfo>;
    chunkserver_manager_->ListChunkServers(chunkservers);
    std::string table_str;
    std::string str =
            "<html><head><title>BFS console</title>\n"
            "<meta http-equiv=\"Content-Type\" content=\"text/html; charset=utf-8\" />\n"
            //"<link rel=\"stylesheet\" type=\"text/css\" "
            //    "href=\"http://www.w3school.com.cn/c5.css\"/>\n"
            //"<style> body { background: #f9f9f9;}"
            //"a:link,a:visited{color:#4078c0;} a:link{text-decoration:none;}"
            //"</style>\n"
            "<script src=\"http://libs.baidu.com/jquery/1.8.3/jquery.min.js\"></script>\n"
            "<link href=\"http://apps.bdimg.com/libs/bootstrap/3.2.0/css/bootstrap.min.css\" "
                "rel=\"stylesheet\">\n"
            "</head>\n";
    str += "<body><div class=\"col-sm-12  col-md-12\">";

    table_str +=
        "<table class=\"table\">"
        "<tr><td>ID</td><td>Address</td><td>Blocks</td><td>Size</td>"
        "<td>Quota</td><td>Used</td><td>Buffers</td>"
        "<td>Tag</td><td>Status</td><td>Check</td><td>Start</td><tr>";
    int dead_num = 0;
    int64_t total_quota = 0;
    int64_t total_data = 0;
    int overladen_num = 0;
    for (int i = 0; i < chunkservers->size(); i++) {
        const ChunkServerInfo& chunkserver = chunkservers->Get(i);
        if (chunkservers->Get(i).is_dead()) {
            dead_num++;
        } else {
            total_quota += chunkserver.disk_quota();
            total_data += chunkserver.data_size();
            if (chunkserver.load() >= kChunkServerLoadMax) {
                overladen_num++;
            }
        }
        if (display_mode == kAliveOnly && chunkservers->Get(i).is_dead()) {
            continue;
        } else if ( display_mode == kDeadOnly && !chunkservers->Get(i).is_dead()) {
            continue;
        } else if (display_mode == kOverload &&
                   (chunkserver.load() < kChunkServerLoadMax ||
                   chunkservers->Get(i).is_dead())) {
            continue;
        }

        table_str += "</td><td>";
        table_str += common::NumToString(chunkserver.id());
        table_str += "</td><td>";
        table_str += "<a href=\"http://" + chunkserver.address() + "/dfs\">"
               + chunkserver.address() + "</a>";
        table_str += "</td><td>";
        table_str += common::NumToString(chunkserver.block_num());
        table_str += "</td><td>";
        table_str += common::HumanReadableString(chunkserver.data_size()) + "B";
        table_str += "</td><td>";
        table_str += common::HumanReadableString(chunkserver.disk_quota()) + "B";
        double ratio = double(chunkserver.data_size()) * 100.0 / double(chunkserver.disk_quota());
        std::string ratio_str = common::NumToString(ratio);
        std::string bg_color = chunkserver.is_dead() ? "background-color:#CCC;" : "";
        std::string style;
        if (ratio >= 94.5) {
            style = "progress-bar-danger";
        } else if (ratio >= 80) {
            style = "progress-bar-warning";
        }
        table_str += "</td><td><div class=\"progress\" style=\"margin-bottom:0\">"
               "<div class=\"progress-bar " + style + "\" "
                    "role=\"progressbar\" aria-valuenow=\"60\" aria-valuemin=\"0\" "
                    "aria-valuemax=\"100\" "
                    "style=\"width: "+ ratio_str + "%; color:#000;" + bg_color + "\">" + ratio_str + "%"
               "</div></div>";
        table_str += "</td><td>";
        table_str += common::NumToString(chunkserver.pending_writes()) + "/" +
                     common::NumToString(chunkserver.buffers());
        table_str += "</td><td>";
        table_str += chunkserver.tag();
        table_str += "</td><td>";
        if (chunkserver.is_dead()) {
            table_str += "dead";
        } else if (chunkserver.kick()) {
            table_str += "kicked";
        } else if (chunkserver.status() == kCsReadonly) {
            table_str += "Readonly";
        } else if (chunkserver.load() >= kChunkServerLoadMax) {
            if (FLAGS_bfs_web_kick_enable) {
                table_str += "overload (<a href=\"/dfs/kick?cs=" +
                             common::NumToString(chunkserver.id())
                        + "\">kick</a>)";
            } else {
                table_str += "overload";
            }
        } else {
            if (FLAGS_bfs_web_kick_enable) {
                table_str += "alive (<a href=\"/dfs/kick?cs=" + common::NumToString(chunkserver.id())
                        + "\">kick</a>)";
            } else {
                table_str += "alive";
            }
        }
        table_str += "</td><td>";
        table_str += common::NumToString(
                        common::timer::now_time() - chunkserver.last_heartbeat());
        table_str += "</td><td>";
        table_str += chunkserver.start_time();
        table_str += "</td></tr>";
    }
    table_str += "</table>";

    RecoverBlockNum recover_num;
    block_mapping_manager_->GetStat(-1, &recover_num);
    int32_t w_qps, r_qps;
    int64_t w_speed, r_speed, recover_speed;
    chunkserver_manager_->GetStat(&w_qps, &w_speed, &r_qps, &r_speed, &recover_speed);
    str += "<h1 style=\"margin-top: 10px; margin-bottom: 0px;\">分布式文件系统控制台 - NameServer</h1>";

    str += "<div class=\"row\">";
    str += "<div class=\"col-sm-6 col-md-6\">";
    str += "<h4 align=left>Nameserver status</h4>";

    str += "<div class=\"col-sm-4 col-md-4\">";
    str += "Total: " + common::HumanReadableString(total_quota) + "B</br>";
    str += "Used: " + common::HumanReadableString(total_data) + "B</br>";
    str += "Pending: (r/w/rp/h)<br>"
        + common::NumToString(read_thread_pool_->PendingNum()) + " "
        + common::NumToString(work_thread_pool_->PendingNum()) + " "
        + common::NumToString(report_thread_pool_->PendingNum()) + " "
        + common::NumToString(heartbeat_thread_pool_->PendingNum()) + "</br>";
    std::string ha_status = sync_ ? sync_->GetStatus() : "none";
    str += "HA status: " + ha_status + "</br>";
    str += "<a href=\"/service?name=baidu.bfs.NameServer\">Rpc status</a>";
    str += "</div>"; // <div class="col-sm-6 col-md-6">

    str += "<div class=\"col-sm-4 col-md-4\">";
    str += "Status: ";
    if (readonly_) {
        str += "<font color=\"red\">Read Only</font></br> <a href=\"/dfs/leave_read_only\">LeaveReadOnly</a>";
    } else {
        str += "Normal</br> <a href=\"/dfs/entry_read_only\">EnterReadOnly</a>";
    }
    str += "</br>";
    if (recover_timeout_ > 1) {
        str += "Recover: " + common::NumToString(recover_timeout_) + "<a href=\"/dfs/stop_recover\"> Stop</a></br>";
    }
    str += "RecoverMode: ";
    if (recover_mode_ == kRecoverAll) {
        str += "RecoverAll</br>";
        str += "<a href=\"/dfs/stop_recover\">StopRecover </a>";
        str += "<a href=\"/dfs/hi_only\">HighOnly</a>";
    } else if (recover_mode_ == kHiOnly) {
        str += "HighOnly</br>";
        str += "<a href=\"/dfs/recover_all\">RecoverAll </a>";
        str += "<a href=\"/dfs/stop_recover\">StopRecover</a>";
    } else {
        str += "<font color=\"red\">NoRecover</font></br>";
        str += " <a href=\"/dfs/hi_only\">HighOnly </a>";
        str += "<a href=\"/dfs/recover_all\">RecoverAll</a>";
    }

    str += "</div>"; // <div class="col-sm-6 col-md-6">

    str += "<div class=\"col-sm-4 col-md-4\">";
    str += "Blocks: " + common::NumToString(g_blocks_num.Get()) + "</br>";
    str += "Recover(hi/lo): " + common::NumToString(recover_num.hi_recover_num) + "/" +
            common::NumToString(recover_num.lo_recover_num) + "</br>";
    str += "Pending: " + common::NumToString(recover_num.hi_pending) + "/" +
            common::NumToString(recover_num.lo_pending) + "</br>";
    str += "Lost: " + common::NumToString(recover_num.lost_num) + "</br>";
    str += "Incomplete: " + common::NumToString(recover_num.incomplete_num) + "</br>";
    str += "<a href=\"/dfs/details\">Details</a>";
    str += "</div>"; // <div class="col-sm-6 col-md-6">
    str += "</div>"; // <div class="col-sm-6 col-md-6">

    str += "<div class=\"col-sm-6 col-md-6\">";
    str += "<h4 align=left>ChunkServer status</h4>";
    str += "<div class=\"col-sm-6 col-md-6\">";
    str += "Total: " + common::NumToString(chunkservers->size())+"</br>";
    str += "Alive: " + common::NumToString(chunkservers->size() - dead_num)+"</br>";
    str += "Dead: " + common::NumToString(dead_num)+"</br>";
    str += "Overload: " + common::NumToString(overladen_num)+"</br>";
    str += "<a href=\"/dfs/alive\">Alive</a>";
    str += "<a href=\"/dfs/dead\"> Dead</a>";
    str += "<a href=\"/dfs/overload\"> Overload</a>";
    str += "<a href=\"/dfs/\"> All</a>";
    str += "</div>"; // <div class="col-sm-6 col-md-6">

    str += "<div class=\"col-sm-6 col-md-6\">";
    str += "w_qps: " + common::NumToString(w_qps)+"</br>";
    str += "w_speed: " + common::HumanReadableString(w_speed)+"</br>";
    str += "r_qps: " + common::NumToString(r_qps)+"</br>";
    str += "r_speed: " + common::HumanReadableString(r_speed)+"</br>";
    str += "recover_speed: " + common::HumanReadableString(recover_speed)+"</p>";
    str += "</div>"; // <div class="col-sm-6 col-md-6">
    str += "</div>"; // <div class="col-sm-6 col-md-6">
    str += "</div>"; // <div class="row">

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
    str += table_str;
    str += "</body></html>";
    delete chunkservers;
    response.content->Append(str);
    return true;
}

static void CallMethodHelper(NameServerImpl* impl,
                             const ::google::protobuf::MethodDescriptor* method,
                             ::google::protobuf::RpcController* controller,
                             const ::google::protobuf::Message* request,
                             ::google::protobuf::Message* response,
                             ::google::protobuf::Closure* done,
                             int64_t recv_time) {
    if (method->index() == 16) {
        int64_t delay = common::timer::get_micros() - recv_time;
        if (delay > FLAGS_block_report_timeout *1000L * 1000L) {
            const BlockReportRequest* report =
                static_cast<const BlockReportRequest*>(request);
            LOG(WARNING, "BlockReport from %s, delay %ld ms",
                report->chunkserver_addr().c_str(), delay / 1000);
            done->Run();
            return;
        }
    }
    impl->NameServer::CallMethod(method, controller, request, response, done);
}

void NameServerImpl::CallMethod(const ::google::protobuf::MethodDescriptor* method,
                                ::google::protobuf::RpcController* controller,
                                const ::google::protobuf::Message* request,
                                ::google::protobuf::Message* response,
                                ::google::protobuf::Closure* done) {
    // the sequence of following list must correspond to the sequence of rpc in
    // 'service NameServer { ... }' at nameserver.proto file
    static std::pair<std::string, ThreadPool*> ThreadPoolOfMethod[] = {
        std::make_pair("CreateFile", work_thread_pool_),
        std::make_pair("AddBlock", work_thread_pool_),
        std::make_pair("GetFileLocation", read_thread_pool_),
        std::make_pair("ListDirectory", read_thread_pool_),
        std::make_pair("Stat", read_thread_pool_),
        std::make_pair("Rename", work_thread_pool_),
        std::make_pair("SyncBlock", work_thread_pool_),
        std::make_pair("FinishBlock", work_thread_pool_),
        std::make_pair("Unlink", work_thread_pool_),
        std::make_pair("DeleteDirectory", work_thread_pool_),
        std::make_pair("ChangeReplicaNum", work_thread_pool_),
        std::make_pair("ShutdownChunkServer", work_thread_pool_),
        std::make_pair("ShutdownChunkServerStat", work_thread_pool_),
        std::make_pair("DiskUsage", read_thread_pool_),
        std::make_pair("Register", work_thread_pool_),
        std::make_pair("HeartBeat", heartbeat_thread_pool_),
        std::make_pair("BlockReport", report_thread_pool_),
        std::make_pair("BlockReceived", work_thread_pool_),
        std::make_pair("PushBlockReport", work_thread_pool_),
        std::make_pair("SysStat", read_thread_pool_)
    };
    static int method_num = sizeof(ThreadPoolOfMethod) /
                            sizeof(std::pair<std::string, ThreadPool*>);
    int id = method->index();
    assert(id < method_num);
    assert(method->name() == ThreadPoolOfMethod[id].first);

    ThreadPool* thread_pool = ThreadPoolOfMethod[id].second;
    if (thread_pool != NULL) {
        int64_t recv_time = common::timer::get_micros();
        std::function<void ()> task =
            std::bind(&CallMethodHelper, this, method, controller,
                        request, response, done, recv_time);
        thread_pool->AddTask(task);
    } else {
        NameServer::CallMethod(method, controller, request, response, done);
    }
}

} // namespace bfs
} // namespace baidu

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
