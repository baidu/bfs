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

namespace baidu {
namespace bfs {

int32_t BfsFile::Write(const char* buf, int32_t len) {
    common::timer::AutoTimer ac;
    int ret = _file->Write(buf, len);
    LOG(INFO, "Write(%s, len: %d) return %d use %.3f ms",
        _name.c_str(), len, ret, ac.TimeUsed() / 1000.0);
    if (ret != len) {
        LOG(INFO, "Write(%s, len: %d) return %d",
            _name.c_str(), len, ret);
    }
    return ret;
}

int32_t BfsFile::Flush() {
    common::timer::AutoTimer ac;
    int ret = -1;
    if (_file->Flush()) {
        ret = 0;
    }
    LOG(INFO, "Flush(%s) return %d use %.3f ms",
        _name.c_str(), ret, ac.TimeUsed() / 1000.0);
    return ret;
}
int32_t BfsFile::Sync() {
    LOG(INFO, "Sync(%s) start", _name.c_str());
    common::timer::AutoTimer ac;
    int ret = -1;
    if (_file->Sync()) {
        ret = 0;
    }
    LOG(INFO, "Sync(%s) return %d usd %.3f ms",
        _name.c_str(), ret, ac.TimeUsed() / 1000.0);
    return ret;
}
int32_t BfsFile::Read(char* buf, int32_t len) {
    common::timer::AutoTimer ac;
    int32_t ret = _file->Read(buf, len);
    LOG(INFO, "Read(%s, len: %d) return %d use %.3f ms",
        _name.c_str(), len, ret, ac.TimeUsed() / 1000.0);
    if (ret != len) {
        //LOG(INFO, "Read(%s, len: %d) return %d",
        //    _name.c_str(), len, ret);
    }
    return ret;
}
int32_t BfsFile::Pread(int64_t offset, char* buf, int32_t len) {
    common::timer::AutoTimer ac;
    int32_t ret = _file->Pread(buf, len, offset, true);
    LOG(INFO, "Pread(%s, offset: %ld, len: %d) return %d use %.3f ms",
        _name.c_str(), offset, len, ret, ac.TimeUsed() / 1000.0);
    return ret;
}
int64_t BfsFile::Tell() {
    common::timer::AutoTimer ac;
    int64_t ret = _file->Seek(0, SEEK_CUR);
    LOG(INFO, "Tell(%s) return %ld use %.3f ms",
        _name.c_str(), ret, ac.TimeUsed() / 1000.0);
    return ret;
}
int32_t BfsFile::Seek(int64_t offset) {
    common::timer::AutoTimer ac;
    int64_t ret = _file->Seek(offset, SEEK_SET);
    if (ret >= 0) {
        ret = 0;
    }
    LOG(INFO, "Seek(%s, %ld) return %ld use %.3f ms",
        _name.c_str(), offset, ret, ac.TimeUsed() / 1000.0);
    return ret;
}
int32_t BfsFile::CloseFile() {
    LOG(INFO, "CloseFile(%s)", _name.c_str());
    common::timer::AutoTimer ac;
    bool ret = _file->Close();
    //delete _file;
    _file = NULL;
    if (!ret) {
        LOG(INFO, "CloseFile(%s) fail", _name.c_str());
        return -1;
    }
    LOG(INFO, "CloseFile(%s) succeed use %.3f ms",
        _name.c_str(), ac.TimeUsed() / 1000.0);
    return 0;
}

BfsImpl::BfsImpl(const std::string& conf) {
    if (!FS::OpenFileSystem(conf.c_str(), &_fs)) {
        assert(0);
    }
}

int32_t BfsImpl::CreateDirectory(const std::string& path) {
    common::timer::AutoTimer ac;
    _fs->CreateDirectory(path.c_str());
    LOG(INFO, "CreateDirectory(%s) use %.3f ms",
        path.c_str(), ac.TimeUsed() / 1000.0);
    return 0;
}
int32_t BfsImpl::DeleteDirectory(const std::string& path) {
    common::timer::AutoTimer ac;
    LOG(INFO, "DeleteDirectory(%s)", path.c_str());
    if (!_fs->DeleteDirectory(path.c_str(), true)) {
        LOG(INFO, "DeleteDirectory(%s) fail", path.c_str());
        return -1;
    }
    LOG(INFO, "DeleteDirectory(%s) succeed use %.3f ms",
        path.c_str(), ac.TimeUsed() / 1000.0);
    return 0;
}
int32_t BfsImpl::Exists(const std::string& filename) {
    common::timer::AutoTimer ac;
    LOG(INFO, "Exists(%s)", filename.c_str());
    if (!_fs->Access(filename.c_str(), 0)) {
        LOG(INFO, "Exists(%s) return false", filename.c_str());
        return -1;
    }
    LOG(INFO, "Exists(%s) return true use %.3f ms",
        filename.c_str(), ac.TimeUsed() / 1000.0);
    return 0;
}
int32_t BfsImpl::Delete(const std::string& filename) {
    common::timer::AutoTimer ac;
    if (!_fs->DeleteFile(filename.c_str())) {
        LOG(INFO, "Delete(%s) fail", filename.c_str());
        return -1;
    }
    LOG(INFO, "Delete(%s) succeed use %.3f ms",
        filename.c_str(), ac.TimeUsed() / 1000.0);
    return 0;
}
int32_t BfsImpl::GetFileSize(const std::string& filename, uint64_t* size) {
    common::timer::AutoTimer ac;
    int64_t file_size = 0;
    if (!_fs->GetFileSize(filename.c_str(), &file_size)) {
        LOG(INFO, "GetFileSize(%s) fail", filename.c_str());
        return -1;
    }
    *size = file_size;
    LOG(INFO, "GetFileSize(%s) return %lu use %.3f",
        filename.c_str(), *size, ac.TimeUsed() / 1000.0);
    return 0;
}
int32_t BfsImpl::Rename(const std::string& from, const std::string& to) {
    common::timer::AutoTimer ac;
    Delete(to);
    if (!_fs->Rename(from.c_str(), to.c_str())) {
        LOG(INFO, "Rename(%s, %s) fail", from.c_str(), to.c_str());
        return -1;
    }
    LOG(INFO, "Rename(%s, %s) succeed use %.3f ms",
        from.c_str(), to.c_str(), ac.TimeUsed() / 1000.0);
    return 0;
}
int32_t BfsImpl::Copy(const std::string& from, const std::string& to) {
    LOG(INFO, "Copy(%s, %s)", from.c_str(), to.c_str());
    abort();
    return 0;
}

int32_t BfsImpl::ListDirectory(const std::string& path, std::vector<std::string>* result) {
    common::timer::AutoTimer ac(100, "ListDirectory", path.c_str());
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
        if (pathname[0] == '\0') continue;
        if (filename == NULL) {
            result->push_back(pathname);
        } else if (filename[0] != '\0' && filename[1] != '\0') {
            result->push_back(filename + 1);
        }
    }
    delete[] files;
    LOG(INFO, "ListDirectory(%s) return %lu items use %.3f ms",
        path.c_str(), result->size(), ac.TimeUsed() / 1000.0);
    return 0;
}

leveldb::DfsFile* BfsImpl::OpenFile(const std::string& filename, int32_t flags) {
    common::timer::AutoTimer ac;
    LOG(INFO, "OpenFile(%s,%d)", filename.c_str(), flags);
    int openflag = O_WRONLY;
    if (leveldb::WRONLY != flags) {
        openflag = O_RDONLY;
    }
    bfs::File* file = NULL;
    if (!_fs->OpenFile(filename.c_str(), openflag, &file)) {
        LOG(WARNING, "OpenFile(%s,%d) fail", filename.c_str(), flags);
        return NULL;
    }
    LOG(INFO, "OpenFile(%s,%d) succeed use %.3f ms",
        filename.c_str(), flags, ac.TimeUsed() / 1000.0);
    return new BfsFile(filename, file);
}

}
} // namespace

extern "C" {

leveldb::Dfs* NewDfs(const char* conf) {
    baidu::common::SetLogFile("./bfslog");
    baidu::common::SetWarningFile("./bfswf");
    return new baidu::bfs::BfsImpl(conf);
}

}
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
