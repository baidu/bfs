// Copyright (c) 2014, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#include <fcntl.h>
#include <stdio.h>
#include <string.h>

#include <string>

#include "common/timer.h"
#include "sdk/bfs.h"

extern std::string FLAGS_nameserver;

void print_usage() {
    printf("Use:\nbfs_client <commond> path\n");
    printf("\t commond:\n");
    printf("\t    -ls <path> : list the directory\n");
    printf("\t    -cat <path> : cat the file\n");
    printf("\t    -mkdir <path> : make director\n");
    printf("\t    -touchz <path> : create a new file\n");
    printf("\t    -rm <path> : remove a file\n");
    printf("\t    -get <bfsfile> <localfile> : copy file to local\n");
    printf("\t    -put <localfile> <bfsfile> : copy file from local to bfs\n");
}

int BfsMkdir(bfs::FS* fs, int argc, char* argv[]) {
    if (argc < 1) {
        print_usage();
        return 1;
    }
    bool ret = fs->CreateDirectory(argv[0]);
    if (!ret) {
        printf("Create dir %s fail\n", argv[0]);
        return 1;
    }
    return 0;
}


int BfsCat(bfs::FS* fs, int argc, char* argv[]) {
    if (argc < 1) {
        print_usage();
        return 1;
    }
    for (int i = 0; i < argc; i++) {
        bfs::File* file;
        if (!fs->OpenFile(argv[i], O_RDONLY, &file)) {
            printf("Can't Open bfs file %s\n", argv[0]);
            return 1;
        }
        char buf[1024];
        int64_t bytes = 0;
        int64_t len = 0;
        while (1) {
            len = file->Read(buf, sizeof(buf));
            if (len <= 0) {
                break;
            }
            bytes += len;
            write(1, buf, len);
        }
        delete file;
    }
    return 0;
}

int BfsGet(bfs::FS* fs, int argc, char* argv[]) {
    if (argc < 2) {
        print_usage();
        return 1;
    }
    bfs::File* file;
    if (!fs->OpenFile(argv[0], O_RDONLY, &file)) {
        printf("Can't Open bfs file %s\n", argv[0]);
        return 1;
    }
    
    FILE* fp = fopen(argv[1], "wb");
    if (fp == NULL) {
        printf("Open local file %s fail\n", argv[1]);
        delete file;
        return -1;
    }
    char buf[1024];
    int64_t bytes = 0;
    int64_t len = 0;
    while (1) {
        len = file->Read(buf, sizeof(buf));
        if (len <= 0) {
            break;
        }
        bytes += len;
        fwrite(buf, len, 1, fp);
    }
    printf("Read %ld bytes from %s\n", bytes, argv[1]);
    delete file;
    return 0;
}

int BfsPut(bfs::FS* fs, int argc, char* argv[]) {
    if (argc != 4) {
        print_usage();
        return 0;
    }
    common::timer::AutoTimer at(0, "BfsPut", argv[3]);
    FILE* fp = fopen(argv[2], "rb");
    if (fp == NULL) {
        printf("Can't open local file %s\n", argv[2]);
        return 1;
    }
    
    bfs::File* file;
    if (!fs->OpenFile(argv[3], O_WRONLY, &file)) {
        printf("Can't Open bfs file %s\n", argv[3]);
        return 1;
    }
    char buf[1024];
    int64_t len = 0;
    int64_t bytes = 0;
    while ( (bytes = fread(buf, 1, sizeof(buf), fp)) > 0) {
        int64_t write_bytes = file->Write(buf, bytes);
        if (write_bytes < bytes) {
            printf("Write fail: [%s:%ld]\n", argv[3], len);
            return 1;
        }
        len += bytes;
    }
    if (!fs->CloseFile(file)) {
        printf("close fail: %s\n", argv[3]);
        return 1;
    }
    delete file;
    printf("Put file to bfs %s %ld bytes\n", argv[3], len);
    return 0;
}

int BfsList(bfs::FS* fs, int argc, char* argv[]) {
    std::string path("/");
    if (argc == 3) {
        path = argv[2];
    }
    bfs::BfsFileInfo* files = NULL;
    int num;
    fs->ListDirectory(path.c_str(), &files, &num);
    printf("Found %d itmes\n", num);
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
        printf("%s\t%s  %s\n", statbuf, timestr, files[i].name);
    }
    delete files;
    return 0;
}

/// bfs client main
int main(int argc, char* argv[])
{
    if (argc < 2) {
        print_usage();
        return 0;
    }
    
    bfs::FS* fs;
    if (!bfs::FS::OpenFileSystem(FLAGS_nameserver.c_str(), &fs)) {
        printf("Open filesytem %s fail\n", FLAGS_nameserver.c_str());
        return 1;
    }

    int ret = 0;
    if (strcmp(argv[1], "-touchz") == 0) {
        if (argc != 3) {
            print_usage();
            return 0;
        }
        bfs::File* file;
        if (!fs->OpenFile(argv[2], O_WRONLY, &file)) {
            printf("Open %s fail\n", argv[2]);
        }
    } else if (strcmp(argv[1], "-rm") == 0) {
        if (argc != 3) {
            print_usage();
            return 0;
        }
        if (fs->DeleteFile(argv[2])) {
            printf("%s Removed\n", argv[2]);
        } else {
            fprintf(stderr, "Remove file fail: %s\n", argv[2]);
        }
    } else if (strcmp(argv[1], "-mkdir") == 0) {
        ret = BfsMkdir(fs, argc - 2, argv + 2);
    } else if (strcmp(argv[1], "-put") == 0) {
        ret = BfsPut(fs, argc, argv);
    } else if (strcmp(argv[1], "-get") == 0 ) {
        ret = BfsGet(fs, argc-2, argv+2);
    } else if (strcmp(argv[1], "-mkdir") == 0) {
        if (argc != 3) {
            print_usage();
            return 0;
        }
        fs->CreateDirectory(argv[2]);
    } else if (strcmp(argv[1], "-cat") == 0) {
        ret = BfsCat(fs, argc - 2, argv + 2);
    } else if (strcmp(argv[1], "-ls") == 0) {
        ret = BfsList(fs, argc, argv);
    } else {
        printf("Unknow common: %s\n", argv[1]);
    }
    return ret;
}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
