// Copyright (c) 2014, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#include "proto/nameserver.pb.h"
#include "proto/chunkserver.pb.h"
#include "rpc/rpc_client.h"
#include "common/mutex.h"
#include "bfs.h"

extern std::string FLAGS_nameserver;

namespace bfs {

struct LocatedBlocks {
    int64_t _file_length;
    std::vector<LocatedBlock> _blocks;
    void CopyFrom(const ::google::protobuf::RepeatedPtrField< ::bfs::LocatedBlock >& blocks) {
        for (int i = 0; i < blocks.size(); i++) {
            _blocks.push_back(blocks.Get(i));
            printf("Block size: %ld\n", blocks.Get(i).block_size());
        }
    }
};

class FSImpl;

class BfsFileImpl : public File {
public:
    BfsFileImpl(FSImpl* fs, const std::string name, int32_t flags)
        : _fs(fs), _name(name), _open_flags(flags), _chains_head(NULL),
        _chunkserver(NULL), _read_offset(0), _closed(false) {}
    ~BfsFileImpl ();
    int64_t Pread(int64_t offset, char* buf, int64_t read_size);
    int64_t Seek(int64_t offset, int32_t whence);
    int64_t Read(char* buf, int64_t read_size);
    int64_t Write(const char* buf, int64_t write_size);
    bool Flush();
    bool Sync();
    friend class FSImpl;
private:
    FSImpl* _fs;                        ///< 文件系统
    std::string _name;                  ///< 文件路径
    int32_t _open_flags;                ///< 打开使用的flag

    /// for write
    ChunkServer_Stub* _chains_head;     ///< 对应的第一个chunkserver
    LocatedBlock* _block_for_write;     ///< 正在写的block

    /// for read
    LocatedBlocks _located_blocks;      ///< block meta for read
    ChunkServer_Stub* _chunkserver;     ///< located chunkserver
    int64_t _read_offset;               ///< 读取的偏移
    bool _closed;                       ///< 是否关闭
    Mutex   _mu;
};

class FSImpl : public FS {
public:
    friend class BfsFileImpl;
    FSImpl() : _rpc_client(NULL), _nameserver(NULL) {
    }
    ~FSImpl() {
        delete _nameserver;
        delete _rpc_client;
    }
    bool ConnectNameServer(const char* nameserver) {
        if (nameserver != NULL) {
            _nameserver_address = nameserver;
        } else {
            _nameserver_address = FLAGS_nameserver;
        }
        _rpc_client = new RpcClient();
        bool ret = _rpc_client->GetStub(_nameserver_address, &_nameserver);
        return ret;
    }
    bool GetFileSize(const char* path, int64_t* file_size) {
        return false;
    }
    bool CreateDirectory(const char* path) {
        CreateFileRequest request;
        CreateFileResponse response;
        request.set_file_name(path);
        request.set_type(0755|(1<<9));
        request.set_sequence_id(0);
        bool ret = _rpc_client->SendRequest(_nameserver, &NameServer_Stub::CreateFile,
            &request, &response, 5, 3);
        if (!ret || response.status() != 0) {
            return false;
        } else {
            return true;
        }
    }
    bool ListDirectory(const char* path, BfsFileInfo** filelist, int *num) {
        *filelist = NULL;
        *num = 0;
        ListDirectoryRequest request;
        ListDirectoryResponse response;
        request.set_path(path);
        request.set_sequence_id(0);
        bool ret = _rpc_client->SendRequest(_nameserver, &NameServer_Stub::ListDirectory,
            &request, &response, 5, 3);
        if (!ret) {
            printf("List fail: %s\n", path);
            return false;
        }
        if (response.files_size() != 0) {
            *num = response.files_size();
            *filelist = new BfsFileInfo[*num];
            for (int i = 0; i < *num; i++) {
                BfsFileInfo& binfo =(*filelist)[i];
                const FileInfo& info = response.files(i);
                binfo.ctime = info.ctime();
                binfo.mode = info.type();
                snprintf(binfo.name, sizeof(binfo.name), "%s", info.name().c_str());
            }
        }
        return true;
    }
    
    bool OpenFile(const char* path, int32_t flags, File** file) {
        bool ret = false;
        *file = NULL;
        if (flags == O_WRONLY) {
            CreateFileRequest request;
            CreateFileResponse response;
            request.set_file_name(path);
            request.set_sequence_id(0);
            request.set_type(0644);
            ret = _rpc_client->SendRequest(_nameserver, &NameServer_Stub::CreateFile,
                &request, &response, 5, 3);
            if (!ret || response.status() != 0) {
                printf("Open file for write fail: %s, status= %d\n", path, response.status());
                ret = false;
            } else {
                printf("Open file %s\n", path);
                *file = new BfsFileImpl(this, path, flags);
            }
        } else if (flags == O_RDONLY) {
            FileLocationRequest request;
            FileLocationResponse response;
            request.set_file_name(path);
            request.set_sequence_id(0);
            ret = _rpc_client->SendRequest(_nameserver, &NameServer_Stub::GetFileLocation,
                &request, &response, 5, 3);
            if (ret && response.status() == 0) {
                BfsFileImpl* f = new BfsFileImpl(this, path, flags);
                f->_located_blocks.CopyFrom(response.blocks());
                *file = f;
            } else {
                printf("GetFileLocation return %d\n", response.blocks_size());
            }
        } else {
            printf("Open flags only O_RDONLY or O_WRONLY\n");
            return false;
        }
        return ret;
    }
    int64_t WriteFile(BfsFileImpl* file, const char* buf, int64_t len) {
        return 0;
    }
    bool DeleteFile(const char* path) {
        return false;
    }
    bool CloseFile(File* file) {
        bool ret = true;
        BfsFileImpl* impl = dynamic_cast<BfsFileImpl*>(file);
        impl->_closed = true;
        return ret;
    }
private:
    RpcClient* _rpc_client;
    NameServer_Stub* _nameserver;
    std::string _nameserver_address;
};

BfsFileImpl::~BfsFileImpl () {
    if (!_closed) {
        _fs->CloseFile(this);
    }
}

int64_t BfsFileImpl::Pread(int64_t offset, char* buf, int64_t read_len) {
    return 0;
}
int64_t BfsFileImpl::Seek(int64_t offset, int32_t whence) {
    if (_open_flags != O_RDONLY) {
        return -2;
    }
    if (whence == SEEK_SET) {
        _read_offset = offset;
    } else if (whence == SEEK_CUR) {
        _read_offset += offset;        
    } else {
        return -1;
    }
    return 0;
}

int64_t BfsFileImpl::Read(char* buf, int64_t read_len) {
    if (_open_flags != O_RDONLY) {
        return -2;
    }
    int ret = Pread(_read_offset, buf, read_len);
    if (ret >= 0) {
        _read_offset += ret;
    }
    return ret;
}
int64_t BfsFileImpl::Write(const char* buf, int64_t write_size) {
    if (_open_flags != O_WRONLY) {
        return -2;
    }
    MutexLock lock(&_mu);
    return _fs->WriteFile(this, buf, write_size);
}
bool BfsFileImpl::Flush() {
    return false;
}
bool BfsFileImpl::Sync() {
    return false;
}

bool FS::OpenFileSystem(const char* nameserver, FS** fs) {
    FSImpl* impl = new FSImpl;
    if (!impl->ConnectNameServer(nameserver)) {
        *fs = NULL;
        return false;
    }
    *fs = impl;
    return true;
}

}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
