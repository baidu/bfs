// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//

#include "fs_impl.h"

#include <gflags/gflags.h>

#include <common/sliding_window.h>
#include <common/logging.h>
#include <common/string_util.h>
#include <common/tprinter.h>
#include <common/util.h>

#include "proto/status_code.pb.h"
#include "proto/chunkserver.pb.h"
#include "rpc/rpc_client.h"
#include "rpc/nameserver_client.h"

#include "file_impl.h"
#include "file_impl_wrapper.h"

DECLARE_int32(sdk_thread_num);
DECLARE_string(nameserver_nodes);
DECLARE_string(sdk_wirte_mode);

namespace baidu {
namespace bfs {

int32_t GetErrorCode(StatusCode stat) {
    if (stat < 100) {
        if (stat == 0) {
            return OK;
        } else {
            return UNKNOWN_ERROR;
        }
    }
    switch (stat / 100) {
        case 1:
            return BAD_PARAMETER;
        case 2:
            return PERMISSION_DENIED;
        case 3:
            return NOT_ENOUGH_QUOTA;
        case 4:
            return NETWORK_UNAVAILABLE;
        case 5:
            return TIMEOUT;
        case 6:
            return NOT_ENOUGH_SPACE;
        case 7:
            return OVERLOAD;
        case 8:
            return META_NOT_AVAILABLE;
        default:
            return UNKNOWN_ERROR;
    }
}

#define MAKE_CASE(name) case name: return (#name)

const char* StrError(int error_code) {
    switch (error_code) {
        MAKE_CASE(OK);
        MAKE_CASE(BAD_PARAMETER);
        MAKE_CASE(PERMISSION_DENIED);
        MAKE_CASE(NOT_ENOUGH_QUOTA);
        MAKE_CASE(NETWORK_UNAVAILABLE);
        MAKE_CASE(TIMEOUT);
        MAKE_CASE(NOT_ENOUGH_SPACE);
        MAKE_CASE(OVERLOAD);
        MAKE_CASE(META_NOT_AVAILABLE);
    }
    return "UNKNOWN_ERROR";
}

FSImpl::FSImpl() : rpc_client_(NULL), nameserver_client_(NULL), leader_nameserver_idx_(0) {
    local_host_name_ = common::util::GetLocalHostName();
    thread_pool_ = new ThreadPool(FLAGS_sdk_thread_num);
}
FSImpl::~FSImpl() {
    delete nameserver_client_;
    delete rpc_client_;
    thread_pool_->Stop(true);
    delete thread_pool_;
}
bool FSImpl::ConnectNameServer(const char* nameserver) {
    std::string nameserver_nodes = FLAGS_nameserver_nodes;
    if (nameserver != NULL) {
        nameserver_nodes = std::string(nameserver);
    }
    rpc_client_ = new RpcClient();
    nameserver_client_ = new NameServerClient(rpc_client_, nameserver_nodes);
    return true;
}
int32_t FSImpl::CreateDirectory(const char* path) {
    CreateFileRequest request;
    CreateFileResponse response;
    request.set_file_name(path);
    request.set_mode(0755|(1<<9));
    request.set_sequence_id(0);
    bool ret = nameserver_client_->SendRequest(&NameServer_Stub::CreateFile,
        &request, &response, 15, 3);
    if (!ret) {
        return TIMEOUT;
    } else if (response.status() != kOK) {
        return GetErrorCode(response.status());
    }
    return OK;
}
int32_t FSImpl::ListDirectory(const char* path, BfsFileInfo** filelist, int *num) {
    common::timer::AutoTimer at(1000, "ListDirectory", path);
    *filelist = NULL;
    *num = 0;
    ListDirectoryRequest request;
    ListDirectoryResponse response;
    request.set_path(path);
    request.set_sequence_id(0);
    bool ret = nameserver_client_->SendRequest(&NameServer_Stub::ListDirectory,
            &request, &response, 60, 1);
    if (!ret || response.status() != kOK) {
        LOG(WARNING, "List fail: %s, ret= %d, status= %s\n",
            path, ret, StatusCode_Name(response.status()).c_str());
        if (!ret) {
            return TIMEOUT;
        } else {
            return GetErrorCode(response.status());
        }
    }
    if (response.files_size() != 0) {
        *num = response.files_size();
        *filelist = new BfsFileInfo[*num];
        for (int i = 0; i < *num; i++) {
            BfsFileInfo& binfo =(*filelist)[i];
            const FileInfo& info = response.files(i);
            binfo.ctime = info.ctime();
            binfo.mode = info.type();
            binfo.size = info.size();
            snprintf(binfo.name, sizeof(binfo.name), "%s", info.name().c_str());
        }
    }
    return OK;
}
int32_t FSImpl::DiskUsage(const char* path, int64_t* du_size) {
    DiskUsageRequest request;
    DiskUsageResponse response;
    request.set_sequence_id(0);
    request.set_path(path);
    bool ret = nameserver_client_->SendRequest(&NameServer_Stub::DiskUsage,
            &request, &response, 3600, 1);
    if (!ret) {
        LOG(WARNING, "Compute Disk Usage fail: %s\n", path);
        return TIMEOUT;
    } else if (response.status() != kOK) {
        return GetErrorCode(response.status());
    }
    *du_size = response.du_size();
    return OK;
}
int32_t FSImpl::DeleteDirectory(const char* path, bool recursive) {
    DeleteDirectoryRequest request;
    DeleteDirectoryResponse response;
    request.set_sequence_id(0);
    request.set_path(path);
    request.set_recursive(recursive);
    bool ret = nameserver_client_->SendRequest(&NameServer_Stub::DeleteDirectory,
            &request, &response, 3600, 1);
    if (!ret) {
        LOG(WARNING, "DeleteDirectory fail: %s\n", path);
        return TIMEOUT;
    } else if (response.status() != kOK) {
        return GetErrorCode(response.status());
    }
    return OK;
}
int32_t FSImpl::Access(const char* path, int32_t mode) {
    StatRequest request;
    StatResponse response;
    request.set_path(path);
    request.set_sequence_id(0);
    bool ret = nameserver_client_->SendRequest(&NameServer_Stub::Stat,
        &request, &response, 15, 1);
    if (!ret) {
        LOG(WARNING, "Stat fail: %s\n", path);
        return TIMEOUT;
    }
    return response.status() == kOK ? 0 : GetErrorCode(response.status());
}
int32_t FSImpl::Stat(const char* path, BfsFileInfo* fileinfo) {
    StatRequest request;
    StatResponse response;
    request.set_path(path);
    request.set_sequence_id(0);
    bool ret = nameserver_client_->SendRequest(&NameServer_Stub::Stat,
        &request, &response, 15, 1);
    if (!ret) {
        LOG(WARNING, "Stat rpc fail: %s", path);
        return TIMEOUT;
    }
    if (response.status() == kOK) {
        const FileInfo& info = response.file_info();
        fileinfo->ctime = info.ctime();
        fileinfo->mode = info.type();
        fileinfo->size = info.size();
        snprintf(fileinfo->name, sizeof(fileinfo->name), "%s", info.name().c_str());
        return OK;
    }
    return GetErrorCode(response.status());
}
int32_t FSImpl::GetFileSize(const char* path, int64_t* file_size) {
    if (file_size == NULL) {
        return BAD_PARAMETER;
    }
    FileLocationRequest request;
    FileLocationResponse response;
    request.set_file_name(path);
    request.set_sequence_id(0);
    bool ret = nameserver_client_->SendRequest(&NameServer_Stub::GetFileLocation,
        &request, &response, 15, 1);
    if (!ret || response.status() != kOK) {
        LOG(WARNING, "GetFileSize(%s) return %s", path, StatusCode_Name(response.status()).c_str());
        if (!ret) {
            return TIMEOUT;
        } else {
            return GetErrorCode(response.status());
        }
    }
    *file_size = 0;
    for (int i = 0; i < response.blocks_size(); i++) {
        const LocatedBlock& block = response.blocks(i);
        if (block.status() != kBlockWriting) {
            *file_size += block.block_size();
            continue;
        }
        ChunkServer_Stub* chunkserver = NULL;
        bool available = false;
        for (int j = 0; j < block.chains_size(); j++) {
            std::string addr = block.chains(j).address();
            ret = rpc_client_->GetStub(addr, &chunkserver);
            if (!ret) {
                LOG(INFO, "GetFileSize(%s) connect chunkserver fail %s",
                    path, addr.c_str());
            } else {
                GetBlockInfoRequest gbi_request;
                gbi_request.set_block_id(block.block_id());
                gbi_request.set_sequence_id(common::timer::get_micros());
                GetBlockInfoResponse gbi_response;
                ret = rpc_client_->SendRequest(chunkserver,
                    &ChunkServer_Stub::GetBlockInfo, &gbi_request, &gbi_response, 15, 3);
                delete chunkserver;
                if (!ret || gbi_response.status() != kOK) {
                    LOG(INFO, "GetFileSize(%s) GetBlockInfo from chunkserver %s fail, ret= %d, status= %s",
                        path, addr.c_str(), ret, StatusCode_Name(gbi_response.status()).c_str());
                    continue;
                }
                *file_size += gbi_response.block_size();
                available = true;
                break;
            }
        }
        if (!available) {
            LOG(WARNING, "GetFileSize(%s) fail no available chunkserver", path);
            return META_NOT_AVAILABLE;
        }
    }
    return OK;
}
int32_t FSImpl::GetFileLocation(const std::string& path,
                     std::map<int64_t, std::vector<std::string> >* locations) {
    if (locations == NULL) {
        return BAD_PARAMETER;
    }
    FileLocationRequest request;
    FileLocationResponse response;
    request.set_file_name(path);
    request.set_sequence_id(0);
    bool ret = nameserver_client_->SendRequest(&NameServer_Stub::GetFileLocation,
                                               &request, &response, 15, 1);
    if (!ret || response.status() != kOK) {
        LOG(WARNING, "GetFileLocation(%s) return %s", path.c_str(),
                StatusCode_Name(response.status()).c_str());
        if (!ret) {
            return TIMEOUT;
        } else {
            return GetErrorCode(response.status());
        }
    }
    for (int i = 0; i < response.blocks_size(); i++) {
        const LocatedBlock& block = response.blocks(i);
        std::map<int64_t, std::vector<std::string> >::iterator it =
            locations->insert(std::make_pair(block.block_id(), std::vector<std::string>())).first;
        for (int j = 0; j < block.chains_size(); ++j) {
            (it->second).push_back(block.chains(j).address());
        }
    }
    return OK;
}
int32_t FSImpl::OpenFile(const char* path, int32_t flags, File** file, const WriteOptions& options) {
    return OpenFile(path, flags, 0, file, options);
}
int32_t FSImpl::OpenFile(const char* path, int32_t flags, int32_t mode,
                         File** file, const WriteOptions& options) {
    if (!(flags & O_WRONLY)) {
        return BAD_PARAMETER;
    }
    WriteOptions write_option = options;
    if (options.write_mode == kWriteDefault) {
        if (FLAGS_sdk_wirte_mode == "fanout") {
            write_option.write_mode = kWriteFanout;
        } else {
            write_option.write_mode = kWriteChains;
        }
    }
    common::timer::AutoTimer at(100, "OpenFile", path);
    int32_t ret = OK;
    *file = NULL;

    CreateFileRequest request;
    CreateFileResponse response;
    request.set_file_name(path);
    request.set_sequence_id(0);
    request.set_flags(flags);
    request.set_mode(mode&0777);
    request.set_replica_num(write_option.replica);
    bool rpc_ret = nameserver_client_->SendRequest(&NameServer_Stub::CreateFile,
        &request, &response, 15, 1);
    if (!rpc_ret || response.status() != kOK) {
        LOG(WARNING, "Open file for write fail: %s, rpc_ret= %d, status= %s\n",
            path, rpc_ret, StatusCode_Name(response.status()).c_str());
        if (!rpc_ret) {
            ret = TIMEOUT;
        } else {
            ret = GetErrorCode(response.status());
        }
    } else {
        *file = new FileImplWrapper(this, rpc_client_, path, flags, write_option);
    }
    return ret;
}
int32_t FSImpl::OpenFile(const char* path, int32_t flags, File** file, const ReadOptions& options) {
    if (flags != O_RDONLY) {
        return BAD_PARAMETER;
    }
    common::timer::AutoTimer at(100, "OpenFile", path);
    int32_t ret = OK;
    *file = NULL;

    FileLocationRequest request;
    FileLocationResponse response;
    request.set_file_name(path);
    request.set_sequence_id(0);
    bool rpc_ret = nameserver_client_->SendRequest(&NameServer_Stub::GetFileLocation,
        &request, &response, 15, 1);
    if (rpc_ret && response.status() == kOK) {
        FileImpl* f = new FileImpl(this, rpc_client_, path, flags, options);
        f->located_blocks_.CopyFrom(response.blocks());
        *file = new FileImplWrapper(f);
    } else {
        LOG(WARNING, "OpenFile return %d, %s\n", ret, StatusCode_Name(response.status()).c_str());
        if (!rpc_ret) {
            ret = TIMEOUT;
        } else {
            ret = GetErrorCode(response.status());
        }
    }
    return ret;
}
int32_t FSImpl::CloseFile(File* file) {
    return file->Close();
}
int32_t FSImpl::DeleteFile(const char* path) {
    UnlinkRequest request;
    UnlinkResponse response;
    request.set_path(path);
    int64_t seq = common::timer::get_micros();
    request.set_sequence_id(seq);
    // printf("Delete file: %s\n", path);
    bool ret = nameserver_client_->SendRequest(&NameServer_Stub::Unlink,
        &request, &response, 15, 1);
    if (!ret) {
        LOG(WARNING, "Unlink rpc fail: %s", path);
        return TIMEOUT;
    }
    if (response.status() != kOK) {
        LOG(WARNING, "Unlink %s return: %s\n", path, StatusCode_Name(response.status()).c_str());
        return GetErrorCode(response.status());
    }
    return OK;
}
int32_t FSImpl::Rename(const char* oldpath, const char* newpath) {
    RenameRequest request;
    RenameResponse response;
    request.set_oldpath(oldpath);
    request.set_newpath(newpath);
    request.set_sequence_id(0);
    bool ret = nameserver_client_->SendRequest(&NameServer_Stub::Rename,
        &request, &response, 15, 1);
    if (!ret) {
        LOG(WARNING, "Rename rpc fail: %s to %s\n", oldpath, newpath);
        return TIMEOUT;
    }
    if (response.status() != kOK) {
        LOG(WARNING, "Rename %s to %s return: %s\n",
            oldpath, newpath, StatusCode_Name(response.status()).c_str());
        return GetErrorCode(response.status());
    }
    return OK;
}
int32_t FSImpl::ChangeReplicaNum(const char* file_name, int32_t replica_num) {
    /*
    ChangeReplicaNumRequest request;
    ChangeReplicaNumResponse response;
    request.set_file_name(file_name);
    request.set_replica_num(replica_num);
    request.set_sequence_id(0);
    bool ret = nameserver_client_->SendRequest(&NameServer_Stub::ChangeReplicaNum,
                                               &request, &response, 15, 1);
    if (!ret) {
        LOG(WARNING, "Change %s replica num to %d rpc fail\n",
                file_name, replica_num);
        return TIMEOUT;
    }
    if (response.status() != kOK) {
        LOG(WARNING, "Change %s replida num to %d return: %s\n",
                file_name, replica_num, StatusCode_Name(response.status()).c_str());
        return GetErrorCode(response.status());
    }
    */
    return PERMISSION_DENIED;
}
int32_t FSImpl::SysStat(const std::string& stat_name, std::string* result) {
    SysStatRequest request;
    SysStatResponse response;
    bool ret = nameserver_client_->SendRequest(&NameServer_Stub::SysStat,
                                               &request, &response, 60, 1);
    if (!ret) {
        LOG(WARNING, "SysStat fail %s", StatusCode_Name(response.status()).c_str());
        return TIMEOUT;
    }
    bool stat_all = (stat_name == "StatAll");
    common::TPrinter tp(8);
    tp.AddRow(8, "", "id", "address", "data_size", "blocks", "tag", "alive", "last_check");
    for (int i = 0; i < response.chunkservers_size(); i++) {
        const ChunkServerInfo& chunkserver = response.chunkservers(i);
        if (!stat_all && chunkserver.is_dead()) {
            continue;
        }
        std::vector<std::string> vs;
        vs.push_back(common::NumToString(i + 1));
        vs.push_back(common::NumToString(chunkserver.id()));
        vs.push_back(chunkserver.address());
        vs.push_back(common::HumanReadableString(chunkserver.data_size()) + "B");
        vs.push_back(common::NumToString(chunkserver.block_num()));
        vs.push_back(chunkserver.tag());
        vs.push_back(chunkserver.is_dead() ? "dead" : "alive");
        vs.push_back(common::NumToString(
                        common::timer::now_time() - chunkserver.last_heartbeat()));
        tp.AddRow(vs);
    }
    /*
    std::ostringstream oss;
    oss << "ChunkServer num: " << response.chunkservers_size() << std::endl
        << "Block num: " << response.block_num() << std::endl;
    result->assign(oss.str());*/
    result->append(tp.ToString());
    return OK;
}
int32_t FSImpl::ShutdownChunkServer(const std::vector<std::string>& cs_addr) {
   ShutdownChunkServerRequest request;
   ShutdownChunkServerResponse response;
   for (size_t i = 0; i < cs_addr.size(); i++) {
       request.add_chunkserver_address(cs_addr[i]);
   }
   bool ret = nameserver_client_->SendRequest(&NameServer_Stub::ShutdownChunkServer,
           &request, &response, 60, 1);
   if (!ret || response.status() != kOK) {
       LOG(WARNING, "Shutdown ChunkServer fail. ret: %d, status: %s",
               ret, StatusCode_Name(response.status()).c_str());
       if (!ret) {
           return TIMEOUT;
       } else {
           return GetErrorCode(response.status());
       }
   }
   return OK;
}
int32_t FSImpl::ShutdownChunkServerStat() {
    ShutdownChunkServerStatRequest request;
    ShutdownChunkServerStatResponse response;
    bool ret = nameserver_client_->SendRequest(&NameServer_Stub::ShutdownChunkServerStat,
                                               &request, &response, 15, 1);
    if (!ret) {
        LOG(WARNING, "Get shutdown chunnkserver stat fail");
        return TIMEOUT;
    }
    return response.in_offline_progress();
}

bool FS::OpenFileSystem(const char* nameserver, FS** fs, const FSOptions&) {
    FSImpl* impl = new FSImpl;
    if (!impl->ConnectNameServer(nameserver)) {
        *fs = NULL;
        return false;
    }
    *fs = impl;
    return true;
}

} // namespace bfs
} // namespace baidu
