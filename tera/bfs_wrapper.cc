// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#include "bfs_wrapper.h"

#include <assert.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <gflags/gflags.h>
#include <bfs.h>
#include "common/logging.h"
#include "common/timer.h"

DECLARE_string(nameserver_nodes);
DECLARE_string(bfs_log);
DECLARE_int32(bfs_log_size);
DECLARE_int32(bfs_log_limit);

namespace baidu {
namespace bfs {

int32_t BfsFile::Write(const char* buf, int32_t len) {
    common::timer::AutoTimer ac;
    int ret = file_->Write(buf, len);
    LOG(INFO, "Write(%s, len: %d) return %d use %.3f ms",
        name_.c_str(), len, ret, ac.TimeUsed() / 1000.0);
    if (ret != len) {
        LOG(INFO, "Write(%s, len: %d) return %d",
            name_.c_str(), len, ret);
    }
    return ret;
}

int32_t BfsFile::Flush() {
    common::timer::AutoTimer ac;
    int ret = file_->Flush();
    LOG(INFO, "Flush(%s) return %d use %.3f ms",
            name_.c_str(), ret, ac.TimeUsed() / 1000.0);
    return ret;
}
int32_t BfsFile::Sync() {
    LOG(INFO, "Sync(%s) start", name_.c_str());
    common::timer::AutoTimer ac;
    int ret = file_->Sync();
    LOG(INFO, "Sync(%s) return %d usd %.3f ms",
            name_.c_str(), ret, ac.TimeUsed() / 1000.0);
    return ret;
}
int32_t BfsFile::Read(char* buf, int32_t len) {
    common::timer::AutoTimer ac;
    int32_t ret = file_->Read(buf, len);
    LOG(INFO, "Read(%s, len: %d) return %d use %.3f ms",
        name_.c_str(), len, ret, ac.TimeUsed() / 1000.0);
    if (ret != len) {
        //LOG(INFO, "Read(%s, len: %d) return %d",
        //    name_.c_str(), len, ret);
    }
    return ret;
}
int32_t BfsFile::Pread(int64_t offset, char* buf, int32_t len) {
    common::timer::AutoTimer ac;
    int32_t ret = file_->Pread(buf, len, offset, true);
    LOG(INFO, "Pread(%s, offset: %ld, len: %d) return %d use %.3f ms",
        name_.c_str(), offset, len, ret, ac.TimeUsed() / 1000.0);
    return ret;
}
int64_t BfsFile::Tell() {
    common::timer::AutoTimer ac;
    int64_t ret = file_->Seek(0, SEEK_CUR);
    LOG(INFO, "Tell(%s) return %ld use %.3f ms",
        name_.c_str(), ret, ac.TimeUsed() / 1000.0);
    return ret;
}
int32_t BfsFile::Seek(int64_t offset) {
    common::timer::AutoTimer ac;
    int64_t ret = file_->Seek(offset, SEEK_SET);
    if (ret >= 0) {
        ret = 0;
    }
    LOG(INFO, "Seek(%s, %ld) return %ld use %.3f ms",
        name_.c_str(), offset, ret, ac.TimeUsed() / 1000.0);
    return ret;
}
int32_t BfsFile::CloseFile() {
    LOG(INFO, "CloseFile(%s)", name_.c_str());
    common::timer::AutoTimer ac;
    int32_t ret = file_->Close();
    delete file_;
    file_ = NULL;
    if (ret != 0) {
        LOG(INFO, "CloseFile(%s) fail", name_.c_str());
        return -1;
    }
    LOG(INFO, "CloseFile(%s) succeed use %.3f ms",
        name_.c_str(), ac.TimeUsed() / 1000.0);
    return 0;
}

BfsImpl::BfsImpl(const std::string& conf) {
    if (!FS::OpenFileSystem(conf.c_str(), &fs_, FSOptions())) {
        assert(0);
    }
}

int32_t BfsImpl::CreateDirectory(const std::string& path) {
    common::timer::AutoTimer ac;
    fs_->CreateDirectory(path.c_str());
    LOG(INFO, "CreateDirectory(%s) use %.3f ms",
        path.c_str(), ac.TimeUsed() / 1000.0);
    return 0;
}
int32_t BfsImpl::DeleteDirectory(const std::string& path) {
    common::timer::AutoTimer ac;
    LOG(INFO, "DeleteDirectory(%s)", path.c_str());
    if (fs_->DeleteDirectory(path.c_str(), true) != 0) {
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
    if (fs_->Access(filename.c_str(), 0) != 0) {
        LOG(INFO, "Exists(%s) return false", filename.c_str());
        return -1;
    }
    LOG(INFO, "Exists(%s) return true use %.3f ms",
        filename.c_str(), ac.TimeUsed() / 1000.0);
    return 0;
}
int32_t BfsImpl::Delete(const std::string& filename) {
    common::timer::AutoTimer ac;
    if (fs_->DeleteFile(filename.c_str()) != 0) {
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
    if (fs_->GetFileSize(filename.c_str(), &file_size) != 0) {
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
    if (fs_->Rename(from.c_str(), to.c_str()) != 0) {
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
    if (fs_->ListDirectory(path.c_str(), &files, &num) != 0) {
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
    bfs::File* file = NULL;
    int ret = -1;
    if (leveldb::WRONLY == flags) {
        ret = fs_->OpenFile(filename.c_str(), O_WRONLY, &file, WriteOptions());
    } else {
        ret = fs_->OpenFile(filename.c_str(), O_RDONLY, &file, ReadOptions());
    }
    if (ret != 0) {
        LOG(WARNING, "OpenFile(%s,%d) fail, ret = %d", filename.c_str(), flags, ret);
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
    const char* internal_conf;
    bool set_log = false;
    if (access(conf, R_OK) == 0) {
        int argc = 2;
        std::string flag = "--flagfile=" + std::string(conf);
        char** argv = new char*[3];
        argv[0] = const_cast<char*>("dummy");
        argv[1] = const_cast<char*>(flag.c_str());
        argv[2] = NULL;
        ::google::ParseCommandLineFlags(&argc, &argv, false);
        delete[] argv;
        internal_conf = FLAGS_nameserver_nodes.c_str();
        if (FLAGS_bfs_log != "") {
            set_log = true;
        }
    } else {
        internal_conf = conf;
    }

    if (set_log) {
        baidu::common::SetLogFile((FLAGS_bfs_log).c_str());
        baidu::common::SetLogSize(FLAGS_bfs_log_size);
        baidu::common::SetLogSizeLimit(FLAGS_bfs_log_limit);
        baidu::common::SetWarningFile("./bfswf");
    } else {
        baidu::common::SetLogFile("./bfslog");
        baidu::common::SetWarningFile("./bfswf");
    }
    return new baidu::bfs::BfsImpl(internal_conf);
}

}
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
