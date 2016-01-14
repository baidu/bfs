// Copyright (c) 2014, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#include "nameserver_impl.h"

#include <set>
#include <map>

#include <boost/bind.hpp>
#include <gflags/gflags.h>
#include <sofa/pbrpc/pbrpc.h>

#include <common/counter.h>
#include <common/logging.h>
#include <common/string_util.h>

#include "nameserver/namespace.h"

DECLARE_int32(nameserver_safemode_time);
DECLARE_int32(chunkserver_max_pending_buffers);

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

NameServerImpl::NameServerImpl() : safe_mode_(true) {
    namespace_ = new NameSpace();
    block_mapping_ = new BlockMapping();
    chunkserver_manager_ = new ChunkServerManager(&thread_pool_, block_mapping_);
    namespace_->RebuildBlockMap(boost::bind(&NameServerImpl::RebuildBlockMapCallback, this, _1));
    thread_pool_.AddTask(boost::bind(&NameServerImpl::LogStatus, this));
    thread_pool_.DelayTask(FLAGS_nameserver_safemode_time * 1000,
        boost::bind(&NameServerImpl::LeaveSafemode, this));
}

NameServerImpl::~NameServerImpl() {
}

void NameServerImpl::LeaveSafemode() {
    LOG(INFO, "Nameserver leave safemode");
    safe_mode_ = false;
}

void NameServerImpl::LogStatus() {
    LOG(INFO, "[Status] create %ld list %ld get_loc %ld add_block %ld "
              "unlink %ld report %ld %ld heartbeat %ld",
        g_create_file.Clear(), g_list_dir.Clear(), g_get_location.Clear(),
        g_add_block.Clear(), g_unlink.Clear(), g_block_report.Clear(),
        g_report_blocks.Clear(), g_heart_beat.Clear());
    thread_pool_.DelayTask(1000, boost::bind(&NameServerImpl::LogStatus, this));
}

void NameServerImpl::HeartBeat(::google::protobuf::RpcController* controller,
                         const HeartBeatRequest* request,
                         HeartBeatResponse* response,
                         ::google::protobuf::Closure* done) {
    g_heart_beat.Inc();
    // printf("Receive HeartBeat() from %s\n", request->data_server_addr().c_str());
    int64_t version = request->namespace_version();
    if (version == namespace_->Version()) {
        chunkserver_manager_->HandleHeartBeat(request, response);
    } else {
        response->set_status(-1);
    }
    response->set_namespace_version(namespace_->Version());
    done->Run();
}

void NameServerImpl::Register(::google::protobuf::RpcController* controller,
                   const ::baidu::bfs::RegisterRequest* request,
                   ::baidu::bfs::RegisterResponse* response,
                   ::google::protobuf::Closure* done) {
    sofa::pbrpc::RpcController* sofa_cntl =
        reinterpret_cast<sofa::pbrpc::RpcController*>(controller);
    const std::string& address = request->chunkserver_addr();
    LOG(INFO, "Register ip: %s", sofa_cntl->RemoteAddress().c_str());
    int64_t version = request->namespace_version();
    if (version != namespace_->Version()) {
        LOG(INFO, "Register from %s version %ld mismatch %ld, remove internal",
            address.c_str(), version, namespace_->Version());
        chunkserver_manager_->RemoveChunkServer(address);
    } else {
        LOG(INFO, "Register from %s, version= %ld", address.c_str(), version);
        chunkserver_manager_->HandleRegister(request, response);
    }
    response->set_namespace_version(namespace_->Version());
    done->Run();
}

void NameServerImpl::BlockReport(::google::protobuf::RpcController* controller,
                   const BlockReportRequest* request,
                   BlockReportResponse* response,
                   ::google::protobuf::Closure* done) {
    g_block_report.Inc();
    int32_t cs_id = request->chunkserver_id();
    LOG(INFO, "Report from %d, %s, %d blocks\n",
        cs_id, request->chunkserver_addr().c_str(), request->blocks_size());
    const ::google::protobuf::RepeatedPtrField<ReportBlockInfo>& blocks = request->blocks();

    int old_id = chunkserver_manager_->GetChunkserverId(request->chunkserver_addr());
    if (cs_id != old_id) {
        LOG(WARNING, "Chunkserver %s id mismatch, old: %d new: %d",
            request->chunkserver_addr().c_str(), old_id, cs_id);
        response->set_status(-1);
        done->Run();
        return;
    }
    for (int i = 0; i < blocks.size(); i++) {
        g_report_blocks.Inc();
        const ReportBlockInfo& block =  blocks.Get(i);
        int64_t cur_block_id = block.block_id();
        int64_t cur_block_size = block.block_size();

        // update block -> cs
        int64_t block_version = block.version();
        if (!block_mapping_->UpdateBlockInfo(cur_block_id, cs_id,
                                             cur_block_size,
                                             block_version,
                                             !safe_mode_ && block_version >= 0)) {
            response->add_obsolete_blocks(cur_block_id);
            chunkserver_manager_->RemoveBlock(cs_id, cur_block_id);
            LOG(INFO, "BlockReport remove obsolete block: #%ld C%d", cur_block_id, cs_id);
            continue;
        }

        // update cs -> block
        chunkserver_manager_->AddBlock(cs_id, cur_block_id);
    }

    // recover replica
    if (!safe_mode_) {
        std::map<int64_t, std::string> recover_blocks;
        chunkserver_manager_->PickRecoverBlocks(cs_id, &recover_blocks);
        for (std::map<int64_t, std::string>::iterator it = recover_blocks.begin();
                it != recover_blocks.end(); ++it) {
            ReplicaInfo* rep = response->add_new_replicas();
            rep->set_block_id(it->first);
            rep->add_chunkserver_address(it->second);
        }
    }
    done->Run();
}

void NameServerImpl::PullBlockReport(::google::protobuf::RpcController* controller,
                   const PullBlockReportRequest* request,
                   PullBlockReportResponse* response,
                   ::google::protobuf::Closure* done) {
    response->set_sequence_id(request->sequence_id());
    response->set_status(0);
    for (int i = 0; i < request->blocks_size(); i++) {
        block_mapping_->ProcessRecoveredBlock(request->chunkserver_id(), request->blocks(i), true);
    }
    for (int i = 0; i < request->failed_size(); i++) {
        block_mapping_->ProcessRecoveredBlock(request->chunkserver_id(), request->failed(i), false);
    }
    done->Run();
}

void NameServerImpl::CreateFile(::google::protobuf::RpcController* controller,
                        const CreateFileRequest* request,
                        CreateFileResponse* response,
                        ::google::protobuf::Closure* done) {
    g_create_file.Inc();
    response->set_sequence_id(request->sequence_id());
    const std::string& file_name = request->file_name();
    int flags = request->flags();
    int mode = request->mode();
    int status = namespace_->CreateFile(file_name, flags, mode);
    response->set_status(status);
    done->Run();
}

void NameServerImpl::AddBlock(::google::protobuf::RpcController* controller,
                         const AddBlockRequest* request,
                         AddBlockResponse* response,
                         ::google::protobuf::Closure* done) {
    g_add_block.Inc();
    response->set_sequence_id(request->sequence_id());
    const std::string& path = request->file_name();
    FileInfo file_info;
    if (!namespace_->GetFileInfo(path, &file_info)) {
        LOG(WARNING, "AddBlock file not found: %s", path.c_str());
        response->set_status(404);
        done->Run();
        return;
    }

    /// replica num
    int replica_num = file_info.replicas();
    /// check lease for write
    std::vector<std::pair<int32_t, std::string> > chains;
    if (chunkserver_manager_->GetChunkServerChains(replica_num, &chains)) {
        int64_t new_block_id = block_mapping_->NewBlockID();
        LOG(INFO, "[AddBlock] new block for %s id= #%ld ",
            path.c_str(), new_block_id);
        LocatedBlock* block = response->mutable_block();
        block_mapping_->AddNewBlock(new_block_id);
        for (int i =0; i<replica_num; i++) {
            ChunkServerInfo* info = block->add_chains();
            info->set_address(chains[i].second);
            LOG(INFO, "Add %s to #%ld response", chains[i].second.c_str(), new_block_id);
            block_mapping_->UpdateBlockInfo(new_block_id, chains[i].first, 0, 0, false);
        }
        block->set_block_id(new_block_id);
        response->set_status(0);
        file_info.add_blocks(new_block_id);
        file_info.set_version(-1);
        ///TODO: Lost update? Get&Update not atomic.
        if (!namespace_->UpdateFileInfo(file_info)) {
            LOG(WARNING, "Update file info fail: %s", path.c_str());
            response->set_status(826);
        }
    } else {
        LOG(INFO, "AddBlock for %s failed.", path.c_str());
        response->set_status(886);
    }
    done->Run();
}

void NameServerImpl::FinishBlock(::google::protobuf::RpcController* controller,
                         const FinishBlockRequest* request,
                         FinishBlockResponse* response,
                         ::google::protobuf::Closure* done) {
    int64_t block_id = request->block_id();
    int64_t block_version = request->block_version();
    response->set_sequence_id(request->sequence_id());
    std::string file_name = request->file_name();
    FileInfo file_info;
    if (!namespace_->GetFileInfo(file_name, &file_info)) {
        LOG(WARNING, "FinishBlock file not found: %s", file_name.c_str());
        response->set_status(404);
        done->Run();
        return;
    }
    file_info.set_version(block_version);
    if (!namespace_->UpdateFileInfo(file_info)) {
        LOG(WARNING, "Update file info fail: %s", file_name.c_str());
        response->set_status(886);
        done->Run();
        return;
    }
    if (!block_mapping_->SetBlockVersion(block_id, block_version)) {
        LOG(WARNING, "Set block version fail: %s", file_name.c_str());
        response->set_status(886);
        done->Run();
        return;
    }
    done->Run();
}

void NameServerImpl::GetFileLocation(::google::protobuf::RpcController* controller,
                      const FileLocationRequest* request,
                      FileLocationResponse* response,
                      ::google::protobuf::Closure* done) {
    response->set_sequence_id(request->sequence_id());
    const std::string& path = request->file_name();
    LOG(INFO, "NameServerImpl::GetFileLocation: %s\n", request->file_name().c_str());
    // Get file_key
    g_get_location.Inc();

    FileInfo info;
    if (!namespace_->GetFileInfo(path, &info)) {
        // No this file
        LOG(INFO, "NameServerImpl::GetFileLocation: NotFound: %s",
            request->file_name().c_str());
        response->set_status(404);
    } else {
        for (int i=0; i<info.blocks_size(); i++) {
            int64_t block_id = info.blocks(i);
            NSBlock nsblock(block_id);
            if (!block_mapping_->GetBlock(block_id, &nsblock)) {
                LOG(WARNING, "GetFileLocation GetBlock fail #%ld ", block_id);
                continue;
            } else {
                LocatedBlock* lcblock = response->add_blocks();
                lcblock->set_block_id(block_id);
                lcblock->set_block_size(nsblock.block_size);
                for (std::set<int32_t>::iterator it = nsblock.replica.begin();
                        it != nsblock.replica.end(); ++it) {
                    int32_t server_id = *it;
                    std::string addr = chunkserver_manager_->GetChunkServerAddr(server_id);
                    if (addr == "") {
                        LOG(INFO, "GetChunkServerAddr from id:%d fail.", server_id);
                        continue;
                    }
                    LOG(INFO, "return server %d %s for #%ld ", server_id, addr.c_str(), block_id);
                    ChunkServerInfo* cs_info = lcblock->add_chains();
                    cs_info->set_address(addr);
                }
            }
        }
        LOG(INFO, "NameServerImpl::GetFileLocation: %s return %d",
            request->file_name().c_str(), info.blocks_size());
        // success if file exist
        response->set_status(0);
    }
    done->Run();
}

void NameServerImpl::ListDirectory(::google::protobuf::RpcController* controller,
                        const ListDirectoryRequest* request,
                        ListDirectoryResponse* response,
                        ::google::protobuf::Closure* done) {
    g_list_dir.Inc();
    response->set_sequence_id(request->sequence_id());
    std::string path = request->path();
    common::timer::AutoTimer at(100, "ListDirectory", path.c_str());

    int status = namespace_->ListDirectory(path, response->mutable_files());
    response->set_status(status);
    done->Run();
}

void NameServerImpl::Stat(::google::protobuf::RpcController* controller,
                          const StatRequest* request,
                          StatResponse* response,
                          ::google::protobuf::Closure* done) {
    response->set_sequence_id(request->sequence_id());
    std::string path = request->path();
    LOG(INFO, "Stat: %s\n", path.c_str());

    FileInfo info;
    if (namespace_->GetFileInfo(path, &info)) {
        FileInfo* out_info = response->mutable_file_info();
        out_info->CopyFrom(info);
        int64_t file_size = 0;
        for (int i = 0; i < out_info->blocks_size(); i++) {
            int64_t block_id = out_info->blocks(i);
            NSBlock nsblock(block_id);
            if (!block_mapping_->GetBlock(block_id, &nsblock)) {
                continue;
            }
            file_size += nsblock.block_size;
        }
        out_info->set_size(file_size);
        response->set_status(0);
        LOG(INFO, "Stat: %s return: %ld", path.c_str(), file_size);
    } else {
        LOG(WARNING, "Stat: %s return: not found", path.c_str());
        response->set_status(404);
    }
    done->Run();
}

void NameServerImpl::Rename(::google::protobuf::RpcController* controller,
                            const RenameRequest* request,
                            RenameResponse* response,
                            ::google::protobuf::Closure* done) {
    response->set_sequence_id(request->sequence_id());
    const std::string& oldpath = request->oldpath();
    const std::string& newpath = request->newpath();

    bool need_unlink;
    FileInfo remove_file;
    int status = namespace_->Rename(oldpath, newpath, &need_unlink, &remove_file);
    if (status == 0 && need_unlink) {
        block_mapping_->RemoveBlocksForFile(remove_file);
    }
    response->set_status(status);
    done->Run();
}

void NameServerImpl::Unlink(::google::protobuf::RpcController* controller,
                            const UnlinkRequest* request,
                            UnlinkResponse* response,
                            ::google::protobuf::Closure* done) {
    g_unlink.Inc();
    response->set_sequence_id(request->sequence_id());
    const std::string& path = request->path();

    FileInfo file_info;
    int status = namespace_->RemoveFile(path, &file_info);
    if (status == 0) {
        block_mapping_->RemoveBlocksForFile(file_info);
    }
    LOG(INFO, "Unlink: %s return %d", path.c_str(), status);
    response->set_status(status);
    done->Run();
}

void NameServerImpl::DeleteDirectory(::google::protobuf::RpcController* controller,
                                     const DeleteDirectoryRequest* request,
                                     DeleteDirectoryResponse* response,
                                     ::google::protobuf::Closure* done)  {
    response->set_sequence_id(request->sequence_id());
    std::string path = request->path();
    bool recursive = request->recursive();
    if (path.empty() || path[0] != '/') {
        response->set_status(886);
        done->Run();
    }
    std::vector<FileInfo> removed;
    int ret_status = namespace_->DeleteDirectory(path, recursive, &removed);
    for (uint32_t i = 0; i < removed.size(); i++) {
        block_mapping_->RemoveBlocksForFile(removed[i]);
    }
    response->set_status(ret_status);
    done->Run();
}

void NameServerImpl::ChangeReplicaNum(::google::protobuf::RpcController* controller,
                                      const ChangeReplicaNumRequest* request,
                                      ChangeReplicaNumResponse* response,
                                      ::google::protobuf::Closure* done) {
    response->set_sequence_id(request->sequence_id());
    std::string file_name = request->file_name();
    int32_t replica_num = request->replica_num();

    int ret_status = 886;

    FileInfo file_info;
    if (namespace_->GetFileInfo(file_name, &file_info)) {
        file_info.set_replicas(replica_num);
        bool ret = namespace_->UpdateFileInfo(file_info);
        assert(ret);
        if (block_mapping_->ChangeReplicaNum(file_info.entry_id(), replica_num)) {
            LOG(INFO, "Change %s replica num to %d", file_name.c_str(), replica_num);
            ret_status = 0;
        } else {
            LOG(WARNING, "Change %s replica num to %d fail", file_name.c_str(), replica_num);
        }
    } else {
        LOG(WARNING, "Change replica num not found: %s", file_name.c_str());
        ret_status = 404;
    }
    response->set_status(ret_status);
    done->Run();
}

void NameServerImpl::RebuildBlockMapCallback(const FileInfo& file_info) {
    for (int i = 0; i < file_info.blocks_size(); i++) {
        int64_t block_id = file_info.blocks(i);
        int64_t version = file_info.version();
        block_mapping_->AddNewBlock(block_id);
        block_mapping_->SetBlockVersion(block_id, version);
        block_mapping_->ChangeReplicaNum(block_id, file_info.replicas());
    }
}

void NameServerImpl::SysStat(::google::protobuf::RpcController* controller,
                             const SysStatRequest* request,
                             SysStatResponse* response,
                             ::google::protobuf::Closure* done) {
    sofa::pbrpc::RpcController* ctl = reinterpret_cast<sofa::pbrpc::RpcController*>(controller);
    LOG(INFO, "SysStat from %s", ctl->RemoteAddress().c_str());
    chunkserver_manager_->ListChunkServers(response->mutable_chunkservers());
    response->set_status(0);
    done->Run();
}

bool NameServerImpl::WebService(const sofa::pbrpc::HTTPRequest& request,
                                sofa::pbrpc::HTTPResponse& response) {
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
        "<td>alive</td><td>last_check</td><tr>";
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
        table_str += "</td><td><div class=\"progress\" style=\"margin-bottom:0\">"
               "<div class=\"progress-bar\" "
                    "role=\"progressbar\" aria-valuenow=\""+ ratio + "\" aria-valuemin=\"0\" "
                    "aria-valuemax=\"100\" style=\"width: "+ ratio + "%\">" + ratio + "%"
               "</div></div>";
        table_str += "</td><td>";
        table_str += common::NumToString(chunkserver.buffers());
        table_str += "</td><td>";
        table_str += chunkserver.is_dead() ? "dead" : "alive";
        table_str += "</td><td>";
        table_str += common::NumToString(
                        common::timer::now_time() - chunkserver.last_heartbeat());
        table_str += "</td></tr>";
    }
    table_str += "</table>";

    int64_t recover_num, pending_num;
    block_mapping_->GetStat(&recover_num, &pending_num);
    str += "<h1>分布式文件系统控制台 - NameServer</h1>";
    str += "<h3 align=left>Nameserver status</h2>";
    str += "<p align=left>Total: " + common::HumanReadableString(total_quota) + "B</br>";
    str += "Used: " + common::HumanReadableString(total_data) + "B</br>";
    str += "Recover: " + common::NumToString(recover_num) + "</br>";
    str += "Pending: " + common::NumToString(pending_num) + "</br>";
    str += "Safemode: " + common::NumToString(safe_mode_) + "</br>";
    str += "Pending tasks: "
        + common::NumToString(thread_pool_.PendingNum()) + "</br>";
    str += "<a href=\"/service?name=baidu.bfs.NameServer\">Rpc status</a></p>";
    str += "<h3 align=left>Chunkserver status</h2>";
    str += "Total: " + common::NumToString(chunkservers->size())+"</br>";
    str += "Alive: " + common::NumToString(chunkservers->size() - dead_num)+"</br>";
    str += "Dead: " + common::NumToString(dead_num)+"</br>";
    str += "Overload: " + common::NumToString(overladen_num)+"</p>";
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
    str += "</div></body></html>";
    delete chunkservers;
    response.content = str;
    return true;
}

} // namespace bfs
} // namespace baidu

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
