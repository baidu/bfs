// Copyright (c) 2014, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#include "nameserver_impl.h"

#include <set>
#include <map>
#include <sstream>

#include <boost/bind.hpp>
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
DECLARE_int32(nameserver_safemode_time);
DECLARE_int32(chunkserver_max_pending_buffers);
DECLARE_int32(nameserver_report_thread_num);
DECLARE_int32(nameserver_work_thread_num);
DECLARE_int32(blockmapping_bucket_num);

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

NameServerImpl::NameServerImpl(Sync* sync) : safe_mode_(FLAGS_nameserver_safemode_time),
    start_recover_(0), sync_(sync) {
    block_mapping_manager_ = new BlockMappingManager(FLAGS_blockmapping_bucket_num);
    report_thread_pool_ = new common::ThreadPool(FLAGS_nameserver_report_thread_num);
    work_thread_pool_ = new common::ThreadPool(FLAGS_nameserver_work_thread_num);
    chunkserver_manager_ = new ChunkServerManager(work_thread_pool_, block_mapping_manager_);
    namespace_ = new NameSpace(false);
    if (sync_) {
        sync_->Init(boost::bind(&NameSpace::TailLog, namespace_, _1));
    }
    CheckLeader();
    start_time_ = common::timer::get_micros();
    work_thread_pool_->AddTask(boost::bind(&NameServerImpl::LogStatus, this));
}

NameServerImpl::~NameServerImpl() {
}

void NameServerImpl::CheckLeader() {
    if (!sync_ || sync_->IsLeader()) {
        LOG(INFO, "Leader nameserver, rebuild block map.");
        NameServerLog log;
        boost::function<void (const FileInfo&)> task =
            boost::bind(&NameServerImpl::RebuildBlockMapCallback, this, _1);
        namespace_->Activate(task, &log);
        if (!LogRemote(log, boost::function<void (bool)>())) {
            LOG(FATAL, "LogRemote namespace update fail");
        }
        safe_mode_ = FLAGS_nameserver_safemode_time;
        start_time_ = common::timer::get_micros();
        work_thread_pool_->DelayTask(1000, boost::bind(&NameServerImpl::CheckSafemode, this));
        is_leader_ = true;
    } else {
        is_leader_ = false;
        work_thread_pool_->DelayTask(100, boost::bind(&NameServerImpl::CheckLeader, this));
        //LOG(INFO, "Delay CheckLeader");
    }
}

void NameServerImpl::CheckSafemode() {
    int now_time = (common::timer::get_micros() - start_time_) / 1000000;
    int safe_mode = safe_mode_;
    if (safe_mode == 0) {
        return;
    }
    int new_safe_mode = FLAGS_nameserver_safemode_time - now_time;
    if (new_safe_mode <= 0) {
        LOG(INFO, "Now time %d", now_time);
        LeaveSafemode();
        return;
    }
    common::atomic_comp_swap(&safe_mode_, new_safe_mode, safe_mode);
    work_thread_pool_->DelayTask(1000, boost::bind(&NameServerImpl::CheckSafemode, this));
}
void NameServerImpl::LeaveSafemode() {
    LOG(INFO, "Nameserver leave safemode");
    block_mapping_manager_->SetSafeMode(false);
    safe_mode_ = 0;
}

void NameServerImpl::LogStatus() {
    LOG(INFO, "[Status] create %ld list %ld get_loc %ld add_block %ld "
              "unlink %ld report %ld %ld heartbeat %ld",
        g_create_file.Clear(), g_list_dir.Clear(), g_get_location.Clear(),
        g_add_block.Clear(), g_unlink.Clear(), g_block_report.Clear(),
        g_report_blocks.Clear(), g_heart_beat.Clear());
    work_thread_pool_->DelayTask(1000, boost::bind(&NameServerImpl::LogStatus, this));
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
        chunkserver_manager_->HandleRegister(cs_ip, request, response);
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
    if (!response->has_sequence_id()) {
        response->set_sequence_id(request->sequence_id());
        boost::function<void ()> task =
            boost::bind(&NameServerImpl::BlockReceived, this, controller, request, response, done);
        work_thread_pool_->AddTask(task);
        return;
    }
    g_block_report.Inc();
    int32_t cs_id = request->chunkserver_id();
    LOG(INFO, "BlockReceived from C%d, %s, %d blocks",
        cs_id, request->chunkserver_addr().c_str(), request->blocks_size());
    const ::google::protobuf::RepeatedPtrField<ReportBlockInfo>& blocks = request->blocks();

    int old_id = chunkserver_manager_->GetChunkServerId(request->chunkserver_addr());
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
        if (block_mapping_manager_->UpdateBlockInfo(block_id, cs_id,
                                            block_size,
                                            block_version)) {
            // update cs -> block
            chunkserver_manager_->AddBlock(cs_id, block_id);
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
    if (!response->has_sequence_id()) {
        int64_t receive_report_time = common::timer::get_micros();
        response->set_sequence_id(receive_report_time);
        boost::function<void ()> task =
            boost::bind(&NameServerImpl::BlockReport, this, controller, request, response, done);
        report_thread_pool_->AddTask(task);
        return;
    }
    int32_t cs_id = request->chunkserver_id();
    LOG(INFO, "Report from C%d %s %d blocks\n",
        cs_id, request->chunkserver_addr().c_str(), request->blocks_size());
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
    int64_t add_time = 0;
    for (int i = 0; i < blocks.size(); i++) {
        g_report_blocks.Inc();
        const ReportBlockInfo& block =  blocks.Get(i);
        int64_t cur_block_id = block.block_id();
        int64_t cur_block_size = block.block_size();

        // update block -> cs
        int64_t block_version = block.version();
        if (!block_mapping_manager_->UpdateBlockInfo(cur_block_id, cs_id,
                                             cur_block_size,
                                             block_version)) {
            response->add_obsolete_blocks(cur_block_id);
            chunkserver_manager_->RemoveBlock(cs_id, cur_block_id);
            LOG(INFO, "BlockReport remove obsolete block: #%ld C%d ", cur_block_id, cs_id);
            continue;
        }

        // update cs -> block
        int64_t before_add_block = common::timer::get_micros();
        chunkserver_manager_->AddBlock(cs_id, cur_block_id);
        int64_t after_add_block = common::timer::get_micros();
        add_time += (after_add_block - before_add_block);
    }
    int64_t after_update = common::timer::get_micros();

    // recover replica
    if (!safe_mode_ && start_recover_) {
        std::vector<std::pair<int64_t, std::vector<std::string> > > recover_blocks;
        int hi_num = 0;
        chunkserver_manager_->PickRecoverBlocks(cs_id, &recover_blocks, &hi_num);
        int32_t priority = 0;
        for (std::vector<std::pair<int64_t, std::vector<std::string> > >::iterator it =
                recover_blocks.begin(); it != recover_blocks.end(); ++it) {
            ReplicaInfo* rep = response->add_new_replicas();
            rep->set_block_id((*it).first);
            rep->set_priority(priority++ < hi_num);
            for (std::vector<std::string>::iterator dest_it = (*it).second.begin();
                 dest_it != (*it).second.end(); ++dest_it) {
                rep->add_chunkserver_address(*dest_it);
            }
        }
        LOG(INFO, "Response to C%d %s new_replicas_size= %d",
            cs_id, request->chunkserver_addr().c_str(), response->new_replicas_size());
    }
    block_mapping_manager_->GetCloseBlocks(cs_id, response->mutable_close_blocks());
    int64_t end_report = common::timer::get_micros();
    if (end_report - start_report > 100 * 1000) {
        LOG(WARNING, "C%d report use %d micors, update use %d micors, add block use %d micors, wait %d micros",
                cs_id, end_report - start_report,
                after_update - before_udpate, add_time, start_report - response->sequence_id());
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
        block_mapping_manager_->ProcessRecoveredBlock(cs_id, request->blocks(i));
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
    if (status != kOK) {
        done->Run();
        return;
    }
    LogRemote(log, boost::bind(&NameServerImpl::SyncLogCallback, this,
                               controller, request, response, done,
                               (std::vector<FileInfo>*)NULL, _1));
}

bool NameServerImpl::LogRemote(const NameServerLog& log, boost::function<void (bool)> callback) {
    if (sync_ == NULL) {
        if (!callback.empty()) {
            work_thread_pool_->AddTask(boost::bind(callback, true));
        }
        return true;
    }
    std::string logstr;
    if (!log.SerializeToString(&logstr)) {
        LOG(FATAL, "Serialize log fail");
    }
    if (callback.empty()) {
        return sync_->Log(logstr);
    } else {
        sync_->Log(logstr, callback);
        return true;
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
            block_mapping_manager_->RemoveBlocksForFile((*removed)[i]);
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
    if (safe_mode_) {
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
        block_mapping_manager_->RemoveBlocksForFile(file_info);
        file_info.clear_blocks();
    }
    /// replica num
    int replica_num = file_info.replicas();
    /// check lease for write
    std::vector<std::pair<int32_t, std::string> > chains;
    if (chunkserver_manager_->GetChunkServerChains(replica_num, &chains, request->client_address())) {
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
            chunkserver_manager_->AddBlock(cs_id, new_block_id);
        }
        block_mapping_manager_->AddNewBlock(new_block_id, replica_num, -1, 0, &replicas);
        block->set_block_id(new_block_id);
        response->set_status(kOK);
        LogRemote(log, boost::bind(&NameServerImpl::SyncLogCallback, this,
                                   controller, request, response, done,
                                   (std::vector<FileInfo>*)NULL, _1));
    } else {
        LOG(WARNING, "AddBlock for %s failed.", path.c_str());
        response->set_status(kGetChunkServerError);
        done->Run();
    }
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
        LOG(DEBUG, "FinishBlock #%ld %s", block_id, file_name.c_str());
        LogRemote(log, boost::bind(&NameServerImpl::SyncLogCallback, this,
                                   controller, request, response, done,
                                   (std::vector<FileInfo>*)NULL, _1));
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
            if (!block_mapping_manager_->GetLocatedBlock(block_id, &replica, &block_size)) {
                LOG(WARNING, "GetFileLocation GetBlockReplica fail #%ld ", block_id);
                break;
            }
            LocatedBlock* lcblock = response->add_blocks();
            lcblock->set_block_id(block_id);
            lcblock->set_block_size(block_size);
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
    if (!response->has_sequence_id()) {
        response->set_sequence_id(request->sequence_id());
        boost::function<void ()> task =
            boost::bind(&NameServerImpl::ListDirectory, this, controller, request, response, done);
        work_thread_pool_->AddTask(task);
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
    LogRemote(log, boost::bind(&NameServerImpl::SyncLogCallback, this,
                               controller, request, response, done, removed, _1));
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
    LOG(INFO, "Unlink: %s return %s", path.c_str(), StatusCode_Name(status).c_str());
    response->set_status(status);
    if (status != kOK) {
        done->Run();
        return;
    }
    std::vector<FileInfo>* removed = new std::vector<FileInfo>;
    removed->push_back(file_info);
    LogRemote(log, boost::bind(&NameServerImpl::SyncLogCallback, this,
                               controller, request, response, done,
                               removed, _1));
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
    }
    std::vector<FileInfo>* removed = new std::vector<FileInfo>;
    NameServerLog log;
    StatusCode ret_status = namespace_->DeleteDirectory(path, recursive, removed, &log);
    response->set_status(ret_status);
    if (ret_status != kOK) {
        done->Run();
        return;
    }
    LogRemote(log, boost::bind(&NameServerImpl::SyncLogCallback, this,
                               controller, request, response, done, removed, _1));
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
        LogRemote(log, boost::bind(&NameServerImpl::SyncLogCallback, this,
                                   controller, request, response, done,
                                   (std::vector<FileInfo>*)NULL, _1));
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
        if (output->size() > 1024) {
            output->append("...");
            break;
        }
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
            "<link href=\"http://apps.bdimg.com/libs/bootstrap/3.2.0/css/bootstrap.min.css\" rel=\"stylesheet\">\n"
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
    if (path == "/dfs/switchtoleader") {
        if (sync_) {
            sync_->SwitchToLeader();
        }
        return true;
    } else if (path == "/dfs/details") {
        ListRecover(&response);
        return true;
    } else if (path == "/dfs/start_recover") {
        start_recover_ = 1;
        response.content->Append("<body onload=\"history.back()\"></body>");
        return true;
    } else if (path == "/dfs/stop_recover") {
        start_recover_ = 0;
        response.content->Append("<body onload=\"history.back()\"></body>");
        return true;
    } else if (path == "/dfs/leave_safemode") {
        LeaveSafemode();
        response.content->Append("<body onload=\"history.back()\"></body>");
        return true;
    } else if (path == "/dfs/enter_safemode") {
        LOG(INFO, "Nameserver enter safemode");
        block_mapping_manager_->SetSafeMode(true);
        safe_mode_ = 1;
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
            "<link href=\"http://apps.bdimg.com/libs/bootstrap/3.2.0/css/bootstrap.min.css\" rel=\"stylesheet\">\n"
            "</head>\n";
    str += "<body><div class=\"col-sm-12  col-md-12\">";

    table_str +=
        "<table class=\"table\">"
        "<tr><td>id</td><td>address</td><td>blocks</td><td>Data size</td>"
        "<td>Disk quota</td><td>Disk used</td><td>Writing buffers</td>"
        "<td>status</td><td>last_check</td><tr>";
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
            if (chunkserver.buffers() > FLAGS_chunkserver_max_pending_buffers * 0.8) {
                overladen_num++;
            }
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
        std::string ratio = common::NumToString(
            chunkserver.data_size() * 100 / chunkserver.disk_quota());
        std::string bg_color = chunkserver.is_dead() ? "background-color:#CCC;" : "";
        table_str += "</td><td><div class=\"progress\" style=\"margin-bottom:0\">"
               "<div class=\"progress-bar\" "
                    "role=\"progressbar\" aria-valuenow=\"60\" aria-valuemin=\"0\" "
                    "aria-valuemax=\"100\" "
                    "style=\"width: "+ ratio + "%; color:#000;" + bg_color + "\">" + ratio + "%"
               "</div></div>";
        table_str += "</td><td>";
        table_str += common::NumToString(chunkserver.pending_writes()) + "/" +
                     common::NumToString(chunkserver.buffers());
        table_str += "</td><td>";
        if (chunkserver.is_dead()) {
            table_str += "dead";
        } else if (chunkserver.kick()) {
            table_str += "kicked";
        } else if (chunkserver.status() == kCsReadonly) {
            table_str += "Readonly";
        } else if (FLAGS_bfs_web_kick_enable) {
            table_str += "alive (<a href=\"/dfs/kick?cs=" + common::NumToString(chunkserver.id())
                      + "\">kick</a>)";
        } else {
            table_str += "alive";
        }
        table_str += "</td><td>";
        table_str += common::NumToString(
                        common::timer::now_time() - chunkserver.last_heartbeat());
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

    str += "<div class=\"col-sm-6 col-md-6\">";
    str += "Total: " + common::HumanReadableString(total_quota) + "B</br>";
    str += "Used: " + common::HumanReadableString(total_data) + "B</br>";
    str += "Safemode: " + common::NumToString(safe_mode_);
    if (safe_mode_) {
        str += " <a href=\"/dfs/leave_safemode\">Leave</a>";
    } else {
        str += " <a href=\"/dfs/enter_safemode\">Enter</a>";
    }
    if (!start_recover_) {
        str += "   <a href=\"/dfs/start_recover\">StartRecover</a>";
    } else {
        str += "   <a href=\"/dfs/stop_recover\">StopRecover</a>";
    }
    str += "</br>";
    str += "Pending tasks: "
        + common::NumToString(work_thread_pool_->PendingNum()) + " "
        + common::NumToString(report_thread_pool_->PendingNum()) + "</br>";
    std::string ha_status = sync_ ? sync_->GetStatus() : "none";
    str += "HA status: " + ha_status + "</br>";
    str += "<a href=\"/service?name=baidu.bfs.NameServer\">Rpc status</a>";
    str += "</div>"; // <div class="col-sm-6 col-md-6">

    str += "<div class=\"col-sm-6 col-md-6\">";
    str += "Recover(hi/lo): " + common::NumToString(recover_num.hi_recover_num) + "/" + common::NumToString(recover_num.lo_recover_num) + "</br>";
    str += "Pending: " + common::NumToString(recover_num.hi_pending) + "/" + common::NumToString(recover_num.lo_pending) + "</br>";
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
    str += "Overload: " + common::NumToString(overladen_num)+"</p>";
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

} // namespace bfs
} // namespace baidu

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
