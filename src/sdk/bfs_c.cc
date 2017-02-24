// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//

#include "sdk/bfs_c.h"
#include "sdk/bfs.h"

#include <gflags/gflags.h>

#include <assert.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <string>
#include <unistd.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <map>

#include <common/string_util.h>
#include <common/timer.h>
#include <common/util.h>


DECLARE_string(flagfile);
DECLARE_string(nameserver_nodes);

extern "C"{

struct bfs_fs_t {
    baidu::bfs::FS*  rep;
};

bfs_fs_t* bfs_open_file_system(const char* flag_file) {
    std::string flag = "--flagfile=./bfs.flag";
    int argc = 1;
    char* file_path = new char[flag.size() + 1];
    strcpy(file_path, flag.c_str());
    char** argv = &file_path;
    ::google::ParseCommandLineFlags(&argc, &argv, false);
    delete[] file_path;

    bfs_fs_t* fs = new bfs_fs_t;
    std::string ns_address = FLAGS_nameserver_nodes;
    bool ret = baidu::bfs::FS::OpenFileSystem(ns_address.c_str(),
            &(fs->rep), baidu::bfs::FSOptions());
    if (!ret) {
        delete fs;
        return NULL;
    }
    return fs;
}

int bfs_create_directory(bfs_fs_t* fs, const char* path) {
    return fs->rep->CreateDirectory(path);
}

int bfs_list_directory(bfs_fs_t* fs, const char* path) {
    char file_types[10] ={'-', 'd', 'l'};
    baidu::bfs::BfsFileInfo* files = NULL;
    int num;
    int32_t ret = fs->rep->ListDirectory(path, &files, &num);
    if (ret != 0) {
        return ret;
    }
    printf("Found %d items\n", num);
    for (int i = 0; i < num; i++) {
        char statbuf[16] = "-rwxrwxrwx";
        int32_t file_type = files[i].mode >> 9;
        int32_t file_perm = files[i].mode & 0777;
        statbuf[0] = file_types[file_type];

        for (int j = 1; j < 10; j++) {
            if ((file_perm & (1<<(9-j))) == 0) {
                statbuf[j] = '-';
            }
        }
        char timestr[64];
        struct tm stm;
        time_t ctime = files[i].ctime;
        localtime_r(&ctime, &stm);
        snprintf(timestr, sizeof(timestr), "%4d-%02d-%02d %2d:%02d",
            stm.tm_year+1900, stm.tm_mon+1, stm.tm_mday, stm.tm_hour, stm.tm_min);
        std::string prefix = path;
        if (files[i].name[0] == '\0') {
            int32_t pos = prefix.size() - 1;
            while (pos >= 0 && prefix[pos] == '/') {
                pos--;
            }
            prefix.resize(pos + 1);
        }
        printf("%s %-9s %s %s%s\n",
               statbuf, baidu::common::HumanReadableString(files[i].size).c_str(),
               timestr, prefix.c_str(), files[i].name);
    }
    delete[] files;
    return 0;
}

int bfs_delete_file(bfs_fs_t* fs, const char* path) {
    return fs->rep->DeleteFile(path);
}

int bfs_rename(bfs_fs_t* fs, const char* oldpath, const char* newpath) {
    return fs->rep->Rename(oldpath, newpath);
}

int bfs_touchz(bfs_fs_t* fs, const char* path) {
    baidu::bfs::File* file;
    int32_t ret = fs->rep->OpenFile(path, O_WRONLY, 0644,
            &file, baidu::bfs::WriteOptions());
    delete file;
    return ret;
}

int bfs_symlink(bfs_fs_t* fs, const char* src, const char* dst) {
    return fs->rep->Symlink(src, dst);
}

int bfs_cat(bfs_fs_t* fs, const char* path) {
    int64_t bytes = 0;
    baidu::bfs::File* file;
    int32_t ret = fs->rep->OpenFile(path, O_RDONLY, &file,
            baidu::bfs::ReadOptions());
    if (ret != 0) {
        return ret;
    }
    char buf[10240];
    int32_t len = 0;
    while (1) {
        len = file->Read(buf, sizeof(buf));
        if (len <= 0) {
            break;
        }
        bytes += len;
        write(1, buf, len);
    }
    delete file;
    return len;
}

int bfs_get(bfs_fs_t* fs, const char* bfs, const char* local) {
    std::string source = bfs;
    std::string target = local;
    std::vector<std::string> src_elements;
    bool src_isdir = false;
    if (!baidu::common::util::SplitPath(source, &src_elements, &src_isdir)
        || src_isdir || src_elements.empty()) {
        return 1;
    }
    std::string src_file_name = src_elements[src_elements.size() - 1];
    if (target.empty() || target[target.size() - 1] == '/') {
        target += src_file_name;
    }
    baidu::common::timer::AutoTimer at(0, "BfsGet", bfs);
    baidu::bfs::File* file;
    if (fs->rep->OpenFile(bfs, O_RDONLY, &file,
                baidu::bfs::ReadOptions()) != 0) {
        return 2;
    }
    FILE* fp = fopen(target.c_str(), "wb");
    if (fp == NULL) {
        delete file;
        return 3;
    }

    char buf[10240];
    int64_t bytes = 0;
    int32_t len = 0;
    while (1) {
        len = file->Read(buf, sizeof(buf));
        if (len <= 0) {
            if (len < 0) {
                fclose(fp);
                return 4;
            }
            break;
        }
        bytes += len;
        fwrite(buf, len, 1, fp);
    }
    printf("Read %ld bytes from %s\n", bytes, source.c_str());
    delete file;
    fclose(fp);
    return len;
}

int bfs_put(bfs_fs_t* fs, const char* local, const char* bfs) {
    std::string source = local;
    std::string target = bfs;
    if (source.empty() || source[source.size() - 1] == '/' || target.empty()) {
        return 1;
    }
    std::string src_file_name;
    size_t pos = source.rfind('/');
    if (pos == std::string::npos) {
        src_file_name = source;
    } else {
        src_file_name = source.substr(pos+1);
    }
    if (target[target.size() - 1] == '/') {
        target += src_file_name;
    }

    baidu::common::timer::AutoTimer at(0, "BfsPut", target.c_str());
    FILE* fp = fopen(local, "rb");
    if (fp == NULL) {
        return 2;
    }
    struct stat st;
    if (stat(local, &st)) {
        fclose(fp);
        return 3;
    }
    baidu::bfs::File* file;
    if (fs->rep->OpenFile(target.c_str(), O_WRONLY | O_TRUNC, st.st_mode,
                &file, baidu::bfs::WriteOptions()) != 0) {
        fclose(fp);
        return 4;
    }
    char buf[10240];
    int64_t len = 0;
    int32_t bytes = 0;
    int ret = 0;
    while ((bytes = fread(buf, 1, sizeof(buf), fp)) > 0) {
        int32_t write_bytes = file->Write(buf, bytes);
        if (write_bytes < bytes) {
            ret = 5;
            break;
        }
        len += bytes;
    }
    fclose(fp);
    if (file->Close() != 0) {
        ret = 6;
    }
    delete file;
    printf("Put file to bfs %s %ld bytes\n", target.c_str(), len);
    return ret;
}

int64_t bfs_du_v2(bfs_fs_t* fs, const char* path) {
    int64_t du_size = 0;
    if (fs->rep->DiskUsage(path, &du_size) != 0) {
        fprintf(stderr, "Compute Disk Usage fail: %s\n", path);
        return -1;
    }
    printf("%-9s\t%s\n",
           baidu::common::HumanReadableString(du_size).c_str(), path);
    return du_size;
}

int bfs_du(bfs_fs_t* fs, const char* path) {
    std::string str_path = path;
    assert(str_path.size() > 0);
    if (str_path[str_path.size() - 1] != '*') {
        if (bfs_du_v2(fs, path) < 0) {
            return -1;
        }
        return 0;
    }

    // Wildcard
    str_path.resize(str_path.size() - 1);
    std::string ppath = str_path.substr(0, str_path.rfind('/') + 1);
    std::string prefix = str_path.substr(ppath.size());
    int num = 0;
    baidu::bfs::BfsFileInfo* files = NULL;
    int32_t ret = fs->rep->ListDirectory(ppath.c_str(), &files, &num);
    if (ret != 0) {
        return ret;
    }
    int64_t total_size = 0;
    for (int j = 0; j < num; j++) {
        std::string name(files[j].name);
        if (name.find(prefix) != std::string::npos) {
            int64_t sz = bfs_du_v2(fs, (ppath + name).c_str());
            if (sz > 0) total_size += sz;
        }
    }
    printf("%s Total: %s\n",
            path, baidu::common::HumanReadableString(total_size).c_str());
    delete[] files;
    return ret;
}

int bfs_rm_dir(bfs_fs_t* fs, const char* path, bool recursive) {
    return fs->rep->DeleteDirectory(path, recursive);
}

int bfs_change_replica_num(bfs_fs_t* fs, const char* path,
        const char* replica_num) {
    if (!isdigit(*replica_num)) {
        return -1;
    }
    return fs->rep->ChangeReplicaNum(path, strtol(replica_num, NULL, 10));
}

int bfs_chmod(bfs_fs_t* fs, const char* str_mode, const char* path) {
    char* end_pos = NULL;
    int32_t mode = strtol(str_mode, &end_pos, 8);
    if (end_pos != NULL) {
        return -1;
    }
    return fs->rep->Chmod(mode, path);
}

int bfs_location(bfs_fs_t* fs, const char* path) {
    std::map<int64_t, std::vector<std::string> > locations;
    int32_t ret = fs->rep->GetFileLocation(path, &locations);
    if (ret != 0) {
        return ret;
    }
    for (std::map<int64_t, std::vector<std::string> >::iterator it =
            locations.begin(); it != locations.end(); ++it) {
        printf("block_id #%ld:\n", it->first);
        for (size_t i = 0; i < (it->second).size(); ++i) {
            printf("%s\n", (it->second)[i].c_str());
        }
    }
    return 0;
}

}
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
