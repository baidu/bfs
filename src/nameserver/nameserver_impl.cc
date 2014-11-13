// Copyright (c) 2014, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#include <set>

#include <sofa/pbrpc/pbrpc.h>
#include <leveldb/db.h>
#include <leveldb/cache.h>

#include "nameserver_impl.h"
#include "common/mutex.h"
#include "common/timer.h"

extern std::string FLAGS_namedb_path;
extern int64_t FLAGS_namedb_cache_size;

namespace bfs {

const uint32_t MAX_PATH_LENGHT = 10240;
const uint32_t MAX_PATH_DEPTH = 99;

/// 构造标准化路径
bool SplitPath(const std::string& path, std::vector<std::string>* element) {
    if (path.empty() || path[0] != '/' || path.size() > MAX_PATH_LENGHT) {
        return false;
    }
    
    char keybuf[MAX_PATH_LENGHT];
    int keylen = 2;
    uint32_t path_depth = 0;
    int last_pos = 0;
    bool valid = true;
    for (size_t i = 0; i <= path.size(); i++) {
        if (i == path.size() || path[i] == '/') {
            if (valid) {
                if (path_depth > MAX_PATH_DEPTH) {
                    return false;
                }
                keybuf[0] = '0' + (path_depth / 10);
                keybuf[1] = '0' + (path_depth % 10);
                memcpy(keybuf + keylen, path.data() + last_pos, i - last_pos);
                keylen += i - last_pos;
                element->push_back(std::string(keybuf, keylen));
                ++path_depth;
            }
            last_pos = i;
            valid = false;
        } else {
            valid = true;
        }
    }
    return true;
}

NameServerImpl::NameServerImpl() {
    leveldb::Options options;
    options.create_if_missing = true;
    options.block_cache = leveldb::NewLRUCache(FLAGS_namedb_cache_size*1024L*1024L);
    leveldb::Status s = leveldb::DB::Open(options, FLAGS_namedb_path, &_db);
    if (!s.ok()) {
        _db = NULL;
        printf("Open leveldb fail: %s\n", s.ToString().c_str());
    }
    _namespace_version = common::timer::get_micros();
}
NameServerImpl::~NameServerImpl() {
}

void NameServerImpl::HeartBeat(::google::protobuf::RpcController* controller,
                         const HeartBeatRequest* request,
                         HeartBeatResponse* response,
                         ::google::protobuf::Closure* done) {
    printf("Receive HeartBeat() from %s\n", 
        request->data_server_addr().c_str());
    int32_t id = request->chunkserver_id();
    int64_t version = request->namespace_version();

    if (version != _namespace_version) {
        // new chunkserver
    } else {
        // handle heartbeat
    }
    response->set_chunkserver_id(id);
    response->set_namespace_version(_namespace_version);
    done->Run();
}

void NameServerImpl::BlockReport(::google::protobuf::RpcController* controller,
                   const BlockReportRequest* request,
                   BlockReportResponse* response,
                   ::google::protobuf::Closure* done) {
    int32_t id = request->chunkserver_id();
    int64_t version = request->namespace_version();
    printf("Report from %d, %d blocks\n", id, request->blocks_size());
    if (version != _namespace_version) {
        response->set_status(8882);
    } else {
        // handle block report
    }
    done->Run();
}

void NameServerImpl::CreateFile(::google::protobuf::RpcController* controller,
                        const CreateFileRequest* request,
                        CreateFileResponse* response,
                        ::google::protobuf::Closure* done) {
    response->set_sequence_id(request->sequence_id());
    const std::string& file_name = request->file_name();
    std::vector<std::string> file_keys;
    if (!SplitPath(file_name, &file_keys)) {
        response->set_status(886);
        done->Run();
        return;
    }
    
    FileInfo file_info;
    std::string info_value;
    int depth = file_keys.size();
    leveldb::Status s;
    for (int i=0; i < depth-1; ++i) {
        s = _db->Get(leveldb::ReadOptions(), file_keys[i], &info_value);
        if (s.IsNotFound()) {
            file_info.set_type((1<<9)|0755);
            //file_info.set_id(0);
            file_info.set_ctime(time(NULL));
            file_info.SerializeToString(&info_value);
            s = _db->Put(leveldb::WriteOptions(), file_keys[i], info_value);
            assert (s.ok());
            printf("Create path recursively: %s\n",file_keys[i].c_str()+2);
        } else {
            bool ret = file_info.ParseFromString(info_value);
            assert(ret);
            if ((file_info.type() & (1<<9)) == 0) {
                printf("Create path fail: %s is not a directory\n", file_keys[i].c_str() + 2);
                response->set_status(886);
                done->Run();
                return;
            }
        }
    }
    
    const std::string& file_key = file_keys[depth-1];
    s = _db->Get(leveldb::ReadOptions(), file_key, &info_value);
    if (s.IsNotFound()) {
        if (request->type() == 0) {
            file_info.set_type(0755);
        } else {
            file_info.set_type(request->type());
        }
        file_info.set_id(0);
        file_info.set_ctime(time(NULL));
        //file_info.add_blocks();
        file_info.SerializeToString(&info_value);
        s = _db->Put(leveldb::WriteOptions(), file_key, info_value);
        if (s.ok()) {
            printf("CreateFile %s\n", file_key.c_str());
            response->set_status(0);
        } else {
            printf("CreateFile %s\n fail: Put fail", file_key.c_str());
            response->set_status(2);
        }
    } else {
        printf("CreateFile %s fail: already exist!\n", file_name.c_str());
        response->set_status(1);
    }
    done->Run();
}

void NameServerImpl::FinishBlock(::google::protobuf::RpcController* controller,
                         const FinishBlockRequest*,
                         FinishBlockResponse*,
                         ::google::protobuf::Closure* done) {
    controller->SetFailed("Method FinishBlock() not implemented.");
    done->Run();
}

void NameServerImpl::GetFileLocation(::google::protobuf::RpcController* controller,
                      const FileLocationRequest* request,
                      FileLocationResponse* response,
                      ::google::protobuf::Closure* done) {
    response->set_sequence_id(request->sequence_id());
    response->set_status(1);
    done->Run();
}

void NameServerImpl::ListDirectory(::google::protobuf::RpcController* controller,
                        const ListDirectoryRequest* request,
                        ListDirectoryResponse* response,
                        ::google::protobuf::Closure* done) {
    response->set_sequence_id(request->sequence_id());
    std::string path = request->path();
    std::vector<std::string> keys;
    if (path.empty() || path[0] != '/') {
        path = "/";
    }
    if (path[path.size()-1] == '/') {
        path += "#";
    }
    if (path.empty() || !SplitPath(path, &keys)) {
        response->set_status(886);
        done->Run();
        return;
    }

    const std::string& file_start_key = keys[keys.size()-1];
    std::string file_end_key = file_start_key;
    if (file_end_key[file_end_key.size()-1] == '#') {
        file_end_key[file_end_key.size()-1] = '\255';
    } else {
        file_end_key += "#";
    }

    printf("List Directory: %s\n", file_start_key.c_str());
    leveldb::Iterator* it = _db->NewIterator(leveldb::ReadOptions());
    for (it->Seek(file_start_key); it->Valid(); it->Next()) {
        leveldb::Slice key = it->key();
        if (key.compare(file_end_key)>=0) {
            break;
        }
        FileInfo* file_info = response->add_files();
        bool ret = file_info->ParseFromArray(it->value().data(), it->value().size());
        assert(ret);
        file_info->set_name(key.data()+2, it->key().size()-2);
    }
    delete it;
    response->set_status(0);
    done->Run();
}

}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
