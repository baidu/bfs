// Copyright (c) 2014, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#include <gflags/gflags.h>

#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <string>
#include <unistd.h>
#include <stdlib.h>

#include <common/util.h>
#include <common/timer.h>
#include "sdk/bfs.h"

DECLARE_string(flagfile);
DECLARE_string(nameserver);
DECLARE_string(nameserver_port);

void print_usage() {
    printf("Use:\nbfs_client <commond> path\n");
    printf("\t commond:\n");
    printf("\t    ls <path> : list the directory\n");
    printf("\t    cat <path> : cat the file\n");
    printf("\t    mkdir <path> : make director\n");
    printf("\t    mv <srcpath> <destpath> : rename director or file\n");
    printf("\t    touchz <path> : create a new file\n");
    printf("\t    rm <path> : remove a file\n");
    printf("\t    get <bfsfile> <localfile> : copy file to local\n");
    printf("\t    put <localfile> <bfsfile> : copy file from local to bfs\n");
    printf("\t    rmdir <path> : remove empty directory\n");
    printf("\t    rmr <path> : remove directory recursively\n");
    printf("\t    change_replica_num <bfsfile> <num>: change replica num of <bfsfile> to <num>\n");
    printf("\t    du <path> : count disk usage for path\n");
    printf("\t    stat : list current stat of the file system\n");
}

int BfsMkdir(baidu::bfs::FS* fs, int argc, char* argv[]) {
    if (argc < 1) {
        print_usage();
        return 1;
    }
    bool ret = fs->CreateDirectory(argv[0]);
    if (!ret) {
        fprintf(stderr, "Create dir %s fail\n", argv[0]);
        return 1;
    }
    return 0;
}

int BfsRename(baidu::bfs::FS* fs, int argc, char* argv[]) {
    if (argc < 2) {
        print_usage();
        return 1;
    }
    bool ret = fs->Rename(argv[0], argv[1]);
    if (!ret) {
        fprintf(stderr, "Rename %s to %s fail\n", argv[0], argv[1]);
        return 1;
    }
    return 0;
}

int BfsCat(baidu::bfs::FS* fs, int argc, char* argv[]) {
    if (argc < 1) {
        print_usage();
        return 1;
    }
    int64_t bytes = 0;
    int32_t len;
    for (int i = 0; i < argc; i++) {
        baidu::bfs::File* file;
        if (!fs->OpenFile(argv[i], O_RDONLY, &file)) {
            fprintf(stderr, "Can't Open bfs file %s\n", argv[0]);
            return 1;
        }
        char buf[10240];
        len = 0;
        while (1) {
            len = file->Read(buf, sizeof(buf));
            if (len <= 0) {
                if (len < 0) {
                    fprintf(stderr, "Read from %s fail.\n", argv[0]);
                }
                break;
            }
            bytes += len;
            write(1, buf, len);
        }
        delete file;
    }
    return len;
}

int BfsGet(baidu::bfs::FS* fs, int argc, char* argv[]) {
    if (argc < 1) {
        print_usage();
        return 1;
    }

    std::string source = argv[0];
    std::string target;
    if (argc >= 2) {
        target = argv[1];
    }
    std::vector<std::string> src_elements;
    bool src_isdir = false;
    if (!baidu::common::util::SplitPath(source, &src_elements, &src_isdir)
        || src_isdir || src_elements.empty()) {
        fprintf(stderr, "Bad file path %s\n", source.c_str());
        return 1;
    }
    std::string src_file_name = src_elements[src_elements.size() - 1];
    if (target.empty() || target[target.size() - 1] == '/') {
        target += src_file_name;
    }

    baidu::common::timer::AutoTimer at(0, "BfsGet", argv[0]);
    baidu::bfs::File* file;
    if (!fs->OpenFile(source.c_str(), O_RDONLY, &file)) {
        fprintf(stderr, "Can't Open bfs file %s\n", source.c_str());
        return 1;
    }

    FILE* fp = fopen(target.c_str(), "wb");
    if (fp == NULL) {
        fprintf(stderr, "Open local file %s fail\n", target.c_str());
        delete file;
        return -1;
    }
    char buf[10240];
    int64_t bytes = 0;
    int32_t len = 0;
    while (1) {
        len = file->Read(buf, sizeof(buf));
        if (len <= 0) {
            if (len < 0) {
                fprintf(stderr, "Read from %s fail.\n", source.c_str());
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

int BfsPut(baidu::bfs::FS* fs, int argc, char* argv[]) {
    if (argc != 4) {
        print_usage();
        return 0;
    }

    std::string source = argv[2];
    std::string target = argv[3];
    if (source.empty() || source[source.size() - 1] == '/' || target.empty()) {
        fprintf(stderr, "Bad file path: %s or %s\n", source.c_str(), target.c_str());
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

    int ret = 0;
    baidu::common::timer::AutoTimer at(0, "BfsPut", target.c_str());
    FILE* fp = fopen(source.c_str(), "rb");
    if (fp == NULL) {
        fprintf(stderr, "Can't open local file %s\n", argv[2]);
        return 1;
    }

    baidu::bfs::File* file;
    ///TODO: Use the same mode as the source file.
    if (!fs->OpenFile(target.c_str(), O_WRONLY | O_TRUNC, 0644, -1, &file)) {
        fprintf(stderr, "Can't Open bfs file %s\n", target.c_str());
        fclose(fp);
        return 1;
    }
    char buf[10240];
    int64_t len = 0;
    int32_t bytes = 0;
    while ( (bytes = fread(buf, 1, sizeof(buf), fp)) > 0) {
        int32_t write_bytes = file->Write(buf, bytes);
        if (write_bytes < bytes) {
            fprintf(stderr, "Write fail: [%s:%ld]\n", target.c_str(), len);
            return 1;
        }
        len += bytes;
    }
    if (!file->Close()) {
        fprintf(stderr, "close fail: %s\n", target.c_str());
        ret = 1;
    }
    delete file;
    fclose(fp);
    printf("Put file to bfs %s %ld bytes\n", target.c_str(), len);
    return ret;
}

int64_t BfsDuRecursive(baidu::bfs::FS* fs, const std::string& path) {
    int64_t ret = 0;
    std::string pad;
    if (path[path.size() - 1] != '/') {
        pad = "/";
    }
    baidu::bfs::BfsFileInfo* files = NULL;
    int num = 0;
    if (!fs->ListDirectory(path.c_str(), &files, &num)) {
        fprintf(stderr, "List directory fail: %s\n", path.c_str());
        return ret;
    }
    for (int i = 0; i < num; i++) {
        std::string file_path = path + pad + files[i].name;
        int32_t type = files[i].mode;
        if (type & (1<<9)) {
            ret += BfsDuRecursive(fs, file_path);
            continue;
        }
        baidu::bfs::BfsFileInfo fileinfo;
        if (fs->Stat(file_path.c_str(), &fileinfo)) {
            ret += fileinfo.size;
            printf("%s\t %ld\n", file_path.c_str(), fileinfo.size);
        }
    }
    delete files;
    return ret;
}

int BfsDu(baidu::bfs::FS* fs, int argc, char* argv[]) {
    if (argc != 1) {
        print_usage();
        return 1;
    }
    int64_t du = BfsDuRecursive(fs, argv[0]);
    printf("Total:\t%ld\n", du);
    return 0;
}

int BfsList(baidu::bfs::FS* fs, int argc, char* argv[]) {
    std::string path("/");
    if (argc == 3) {
        path = argv[2];
        if (path.size() && path[path.size()-1] != '/') {
            path.append("/");
        }
    }
    baidu::bfs::BfsFileInfo* files = NULL;
    int num;
    bool ret = fs->ListDirectory(path.c_str(), &files, &num);
    if (!ret) {
        fprintf(stderr, "List dir %s fail\n", path.c_str());
        return 1;
    }
    printf("Found %d items\n", num);
    for (int i = 0; i < num; i++) {
        int32_t type = files[i].mode;
        char statbuf[16] = "drwxrwxrwx";
        for (int j = 0; j < 10; j++) {
            if ((type & (1<<(9-j))) == 0) {
                statbuf[j] = '-';
            }
        }
        char timestr[64];
        struct tm stm;
        time_t ctime = files[i].ctime;
        localtime_r(&ctime, &stm);
        snprintf(timestr, sizeof(timestr), "%4d-%02d-%02d %2d:%02d",
            stm.tm_year+1900, stm.tm_mon+1, stm.tm_mday, stm.tm_hour, stm.tm_min);
        printf("%s\t%s  %s%s\n", statbuf, timestr, path.c_str(), files[i].name);
    }
    delete files;
    return 0;
}

int BfsRmdir(baidu::bfs::FS* fs, int argc, char* argv[], bool recursive) {
    if (argc < 1) {
        print_usage();
        return 1;
    }
    bool ret = fs->DeleteDirectory(argv[0], recursive);
    if (!ret) {
        fprintf(stderr, "Remove dir %s fail\n", argv[0]);
        return 1;
    }
    return 0;
}

int BfsChangeReplicaNum(baidu::bfs::FS* fs, int argc, char* argv[]) {
    if (argc < 2) {
        print_usage();
        return 1;
    }
    char* file_name = argv[0];
    int32_t replica_num = atoi(argv[1]);
    bool ret = fs->ChangeReplicaNum(file_name, replica_num);
    if (!ret) {
        fprintf(stderr, "Change %s replica num to %d fail\n", file_name, replica_num);
        return 1;
    }
    return 0;
}

int BfsStat(baidu::bfs::FS* fs, int argc, char* argv[]) {
    std::string stat_name("Stat");
    if (argc && 0 == strcmp(argv[0], "-a")) {
        stat_name = "StatAll";
    }
    std::string result;
    bool ret = fs->SysStat(stat_name, &result);
    if (!ret) {
        fprintf(stderr, "SysStat fail\n");
        return 1;
    }
    printf("%s\n", result.c_str());
    return 0;
}

/// bfs client main
int main(int argc, char* argv[]) {
    FLAGS_flagfile = "./bfs.flag";
    int gflags_argc = 1;
    ::google::ParseCommandLineFlags(&gflags_argc, &argv, false);

    if (argc < 2) {
        print_usage();
        return 0;
    }

    baidu::bfs::FS* fs;
    std::string ns_address = FLAGS_nameserver + ":" + FLAGS_nameserver_port;
    if (!baidu::bfs::FS::OpenFileSystem(ns_address.c_str(), &fs)) {
        fprintf(stderr, "Open filesytem %s fail\n", ns_address.c_str());
        return 1;
    }

    int ret = 1;
    if (strcmp(argv[1], "touchz") == 0) {
        if (argc != 3) {
            print_usage();
            return ret;
        }
        baidu::bfs::File* file;
        if (!fs->OpenFile(argv[2], O_WRONLY, 644, -1, &file)) {
            fprintf(stderr, "Open %s fail\n", argv[2]);
        } else {
            ret = 0;
        }
    } else if (strcmp(argv[1], "rm") == 0) {
        if (argc != 3) {
            print_usage();
            return ret;
        }
        if (fs->DeleteFile(argv[2])) {
            printf("%s Removed\n", argv[2]);
            ret = 0;
        } else {
            fprintf(stderr, "Remove file fail: %s\n", argv[2]);
        }
    } else if (strcmp(argv[1], "mkdir") == 0) {
        ret = BfsMkdir(fs, argc - 2, argv + 2);
    } else if (strcmp(argv[1], "mv") == 0) {
        ret = BfsRename(fs, argc - 2, argv + 2);
    } else if (strcmp(argv[1], "put") == 0) {
        ret = BfsPut(fs, argc, argv);
    } else if (strcmp(argv[1], "get") == 0 ) {
        ret = BfsGet(fs, argc - 2, argv + 2);
    } else if (strcmp(argv[1], "cat") == 0) {
        ret = BfsCat(fs, argc - 2, argv + 2);
    } else if (strcmp(argv[1], "ls") == 0) {
        ret = BfsList(fs, argc, argv);
    } else if (strcmp(argv[1], "rmdir") == 0) {
        ret = BfsRmdir(fs, argc - 2, argv + 2, false);
    } else if (strcmp(argv[1], "rmr") == 0) {
        ret = BfsRmdir(fs, argc - 2, argv + 2, true);
    } else if (strcmp(argv[1], "change_replica_num") == 0) {
        ret = BfsChangeReplicaNum(fs, argc - 2, argv + 2);
    } else if (strcmp(argv[1], "du") == 0) {
        ret = BfsDu(fs, argc - 2, argv + 2);
    } else if (strcmp(argv[1], "stat") == 0) {
        ret = BfsStat(fs, argc - 2, argv + 2);
    } else {
        fprintf(stderr, "Unknow common: %s\n", argv[1]);
    }
    return ret;
}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
