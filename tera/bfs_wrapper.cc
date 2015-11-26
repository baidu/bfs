// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#include "bfs_wrapper.h"

#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <bfs.h>
#include "common/logging.h"
#include "common/timer.h"

namespace bfs {

int32_t BfsFile::Write(const char* buf, int32_t len) {
    common::timer::AutoTimer ac(0.1, "BfsFile::Write", _name.c_str());
    int ret = _file->Write(buf, len);
    LOG(INFO, "Write(%s, len: %d) return %d", _name.c_str(), len, ret);
    if (ret != len) {
        LOG(INFO, "Write(%s, len: %d) return %d",
            _name.c_str(), len, ret);
    }
    return ret;
}

int32_t BfsFile::Flush() {
    common::timer::AutoTimer ac(1, "Flush", _name.c_str());
    int ret = -1;
    if (_file->Flush()) {
        ret = 0;
    }
    LOG(INFO, "Flush(%s) return %d", _name.c_str(), ret);
    return ret;
}
int32_t BfsFile::Sync() {
    LOG(INFO, "Sync(%s) start", _name.c_str());
    common::timer::AutoTimer ac(10, "Sync", _name.c_str());
    int ret = -1;
    if (_file->Sync()) {
        ret = 0;
    }
    LOG(INFO, "Sync(%s) return %d", _name.c_str(), ret);
    return ret;
}
int32_t BfsFile::Read(char* buf, int32_t len) {
    common::timer::AutoTimer ac(1, "Read", _name.c_str());
    int32_t ret = _file->Read(buf, len);
    LOG(INFO, "Read(%s, len: %d) return %d", _name.c_str(), len, ret);
    if (ret != len) {
        LOG(INFO, "Read(%s, len: %d) return %d",
            _name.c_str(), len, ret);
    }
    return ret;
}
int32_t BfsFile::Pread(int64_t offset, char* buf, int32_t len) {
    common::timer::AutoTimer ac(1, "Pread", _name.c_str());
    int32_t ret = _file->Pread(buf, len, offset, true);
    LOG(INFO, "Pread(%s, offset: %ld, len: %d) return %d",
        _name.c_str(), offset, len, ret);
    return ret;
}
int64_t BfsFile::Tell() {
    common::timer::AutoTimer ac(1, "Tell", _name.c_str());
    int64_t ret = _file->Seek(0, SEEK_CUR);
    LOG(INFO, "Tell(%s) return %ld", _name.c_str(), ret);
    return ret;
}
int32_t BfsFile::Seek(int64_t offset) {
    common::timer::AutoTimer ac(1, "Seek", _name.c_str());
    int64_t ret = _file->Seek(offset, SEEK_SET);
    if (ret >= 0) {
        ret = 0;
    }
    LOG(INFO, "Seek(%s, %ld) return %ld", _name.c_str(), offset, ret);
    return ret;
}
int32_t BfsFile::CloseFile() {
    LOG(INFO, "CloseFile(%s)", _name.c_str());
    common::timer::AutoTimer ac(1, "CloseFile", _name.c_str());
    bool ret = _file->Close();
    delete _file;
    _file = NULL;
    if (!ret) {
        LOG(INFO, "CloseFile(%s) fail", _name.c_str());
        return -1;
    }
    LOG(INFO, "CloseFile(%s) succeed", _name.c_str());
    return 0;
}

BfsImpl::BfsImpl(const std::string& conf) {
    if (!FS::OpenFileSystem(conf.c_str(), &_fs)) {
        assert(0);
    }
}

int32_t BfsImpl::CreateDirectory(const std::string& path) {
    LOG(INFO, "CreateDirectory(%s)", path.c_str());
    _fs->CreateDirectory(path.c_str());
    return 0;
}
int32_t BfsImpl::DeleteDirectory(const std::string& path) {
    LOG(INFO, "DeleteDirectory(%s)", path.c_str());
    if (!_fs->DeleteDirectory(path.c_str(), true)) {
        LOG(INFO, "DeleteDirectory(%s) fail", path.c_str());
        return -1;
    }
    LOG(INFO, "DeleteDirectory(%s) succeed", path.c_str());
    return 0;
}
int32_t BfsImpl::Exists(const std::string& filename) {
    LOG(INFO, "Exists(%s)", filename.c_str());
    if (!_fs->Access(filename.c_str(), 0)) {
        LOG(INFO, "Exists(%s) return false", filename.c_str());
        return -1;
    }
    LOG(INFO, "Exists(%s) return true", filename.c_str());
    return 0;
}
int32_t BfsImpl::Delete(const std::string& filename) {
    common::timer::AutoTimer ac(1, "Delete", filename.c_str());
    if (!_fs->DeleteFile(filename.c_str())) {
        LOG(INFO, "Delete(%s) fail", filename.c_str());
        return -1;
    }
    LOG(INFO, "Delete(%s) succeed", filename.c_str());
    return 0;
}
int32_t BfsImpl::GetFileSize(const std::string& filename, uint64_t* size) {
    int64_t file_size = 0;
    if (!_fs->GetFileSize(filename.c_str(), &file_size)) {
        LOG(INFO, "GetFileSize(%s) fail", filename.c_str());
        return -1;
    }
    *size = file_size;
    LOG(INFO, "GetFileSize(%s) return %lu", filename.c_str(), *size);
    return 0;
}
int32_t BfsImpl::Rename(const std::string& from, const std::string& to) {
    Delete(to);
    if (!_fs->Rename(from.c_str(), to.c_str())) {
        LOG(INFO, "Rename(%s, %s) fail", from.c_str(), to.c_str());
        return -1;
    }
    LOG(INFO, "Rename(%s, %s) succeed", from.c_str(), to.c_str());
    return 0;
}
int32_t BfsImpl::Copy(const std::string& from, const std::string& to) {
    LOG(INFO, "Copy(%s, %s)", from.c_str(), to.c_str());
    abort();
    return 0;
}

int32_t BfsImpl::ListDirectory(const std::string& path, std::vector<std::string>* result) {
    common::timer::AutoTimer ac(1, "ListDirectory", path.c_str());
    LOG(INFO, "ListDirectory(%s)", path.c_str());
    if (result == NULL) {
        return -1;
    }
    bfs::BfsFileInfo* files = NULL;
    int num = 0;
    if (!_fs->ListDirectory(path.c_str(), &files, &num)) {
        return -1;
    }
    for (int i = 0; i < num; i++) {
        char* pathname = files[i].name;
        char* filename = rindex(pathname, '/');
        if (filename != NULL && filename[0] != '\0') {
            result->push_back(filename + 1);
        }
    }
    delete[] files;
    LOG(INFO, "ListDirectory(%s) return %lu items", path.c_str(), result->size());    
    return 0;
}

leveldb::DfsFile* BfsImpl::OpenFile(const std::string& filename, int32_t flags) {
    common::timer::AutoTimer ac(0, "OpenFile", filename.c_str());
    LOG(INFO, "OpenFile(%s,%d)", filename.c_str(), flags);
    int openflag = O_WRONLY;
    if (leveldb::WRONLY != flags) {
        openflag = O_RDONLY;
    }
    bfs::File* file = NULL;
    if (!_fs->OpenFile(filename.c_str(), openflag, &file)) {
        return NULL;
    }
    LOG(INFO, "OpenFile(%s,%d) succeed", filename.c_str(), flags);
    return new BfsFile(filename, file);
}

} // namespace

extern "C" {

leveldb::Dfs* NewDfs(const char* conf) {
    common::SetLogFile("./bfslog");
    common::SetWarningFile("./bfswf");
    return new bfs::BfsImpl(conf);
}

}
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
