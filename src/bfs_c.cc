/***************************************************************************
 *
 * Copyright (c) 2017 Baidu.com, Inc. All Rights Reserved
 *
 **************************************************************************/



/**
 * @file bfs_c.cc
 * @author sunjinjin01(com@baidu.com)
 * @date 2017/01/03 16:33:59
 * @brief
 *
 **/


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


#include "sdk/bfs_c.h"
#include "sdk/bfs.h"
//using baidu::bfs::WriteBuff;
//using baidu::bfs::FS;

DECLARE_string(flagfile);
DECLARE_string(nameserver_nodes);

extern "C"{
struct FSOptions {
    const char* username;
    const char* passwd;
    FSOptions() : username(NULL), passwd(NULL) {}

};
struct bfs_fs_t { baidu::bfs::FS*  rep; };

bfs_fs_t* bfs_open_file_system(){
    FLAGS_flagfile = "./bfs.flag";
    bfs_fs_t* fs = new bfs_fs_t;
    std::string ns_address = FLAGS_nameserver_nodes;
    bool responce = baidu::bfs::FS::OpenFileSystem(ns_address.c_str(), &(fs->rep), baidu::bfs::FSOptions());
    if (!responce || !(fs->rep)) {
        delete fs;
        return NULL;
    }
    return fs;

}
int32_t bfs_create_directory(bfs_fs_t* fs, const char* path){
    int result = fs->rep->CreateDirectory(path);
    printf("result:%d", result);
    return result;
}
}




















/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
