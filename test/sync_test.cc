// Copyright (c) 2014, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <gflags/gflags.h>

#include <bfs.h>

DECLARE_string(nameserver);
DECLARE_string(nameserver_port);

int main() {
    bfs::FS *fs = NULL;
    std::string ns_addr = FLAGS_nameserver + ":" + FLAGS_nameserver_port;
    if (!bfs::FS::OpenFileSystem(ns_addr.c_str(), &fs)) {
        printf("Open fs fail\n");
        return 1;
    }

    bfs::File *file = NULL;
    bool ret = fs->OpenFile("/synctest", O_WRONLY, &file);
    assert(ret);
    const char* hw = "Hello world~\n";
    file->Write(hw, strlen(hw));
    file->Sync();

    ret = fs->OpenFile("/synctest", O_RDONLY, &file);
    assert(ret);
    char buff[64] = {};
    int64_t len = file->Read(buff, 1024);
    printf("Read return %ld, buff: %s\n", len, buff);
    return 0;
}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
