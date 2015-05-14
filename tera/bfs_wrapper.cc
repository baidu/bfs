// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#include "bfs_wrapper.h"

#include <assert.h>
#include <bfs.h>

namespace bfs {

int32_t BfsFile::Write(const char* buf, int32_t len) {
    return _file->Write(buf, len);
}
int32_t BfsFile::Flush() {
    if (!_file->Flush()) {
        return -1;
    }
    return 0;
}
int32_t BfsFile::Sync() {
    if (!_file->Sync()) {
        return -1;
    }
    return 0;
}
int32_t BfsFile::Read(char* buf, int32_t len) {
    return _file->Read(buf, len);
}
int32_t BfsFile::Pread(int64_t offset, char* buf, int32_t len) {
    return _file->Pread(buf, len, offset);
}
int64_t BfsFile::Tell() {
    return _file->Seek(0, SEEK_CUR);
}
int32_t BfsFile::Seek(int64_t offset) {
    int64_t ret = _file->Seek(offset, SEEK_SET);
    if (ret >= 0) {
        return 0;
    }
    return ret;
}
int32_t BfsFile::CloseFile() {
    bool ret = _file->Close();
    delete _file;
    _file = NULL;
    if (!ret) {
        return -1;
    }
    return 0;
}


BfsImpl::BfsImpl(const std::string& conf) {
    if (!FS::OpenFileSystem(conf.c_str(), &_fs)) {
        assert(0);
    }
}

int32_t BfsImpl::CreateDirectory(const std::string& path) {
    _fs->CreateDirectory(path.c_str());
    return 0;
}
int32_t BfsImpl::DeleteDirectory(const std::string& path) {
    if (!_fs->DeleteDirectory(path.c_str(), true)) {
        return -1;
    }
    return 0;
}
int32_t BfsImpl::Exists(const std::string& filename) {
    if (!_fs->Access(filename.c_str(), 0)) {
        return -1;
    }
    return 0;
}
int32_t BfsImpl::Delete(const std::string& filename) {
    if (!_fs->DeleteFile(filename.c_str())) {
        return -1;
    }
    return 0;
}
int32_t BfsImpl::GetFileSize(const std::string& filename, uint64_t* size) {
    BfsFileInfo fileinfo;
    if (!_fs->Stat(filename.c_str(), &fileinfo)) {
        return -1;
    }
    *size = fileinfo.size;
    return 0;
}
int32_t BfsImpl::Rename(const std::string& from, const std::string& to) {
    if (!_fs->Rename(from.c_str(), to.c_str())) {
        return -1;
    }
    return 0;
}
int32_t BfsImpl::Copy(const std::string& from, const std::string& to) {
    return 0;
}

int32_t BfsImpl::ListDirectory(const std::string& path, std::vector<std::string>* result) {
    if (result == NULL) {
        return -1;
    }
    bfs::BfsFileInfo* files = NULL;
    int num = 0;
    if (!_fs->ListDirectory(path.c_str(), &files, &num)) {
        return -1;
    }
    for (int i = 0; i < num; i++) {
        result->push_back(files[i].name);
    }
    delete files;
    return 0;
}

leveldb::DfsFile* BfsImpl::OpenFile(const std::string& filename, int32_t flags) {
    int openflag = O_WRONLY;
    if (leveldb::WRONLY != flags) {
        openflag = O_RDONLY;
    }
    bfs::File* file = NULL;
    if (!_fs->OpenFile(filename.c_str(), openflag, &file)) {
        return NULL;
    }
    return new BfsFile(file);
}

} // namespace

extern "C" {

leveldb::Dfs* NewDfs(const char* conf) {
    return new bfs::BfsImpl(conf);
}

}
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
