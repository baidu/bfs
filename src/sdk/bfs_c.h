/***************************************************************************
 *
 * Copyright (c) 2017 Baidu.com, Inc. All Rights Reserved
 *
 **************************************************************************/



/**
 * @file bfs_c.h
 * @author sunjinjin01(com@baidu.com)
 * @date 2017/01/03 15:38:19
 * @brief
 *
 **/




#ifndef  __BFS_C_H_
#define  __BFS_C_H_
#include "bfs.h"
#pragma GCC visibility push (default)
#ifdef __cplusplus
extern "C"{
#endif

//typedef struct bfs_rpc_client_t bfs_rpc_client;
//typedef struct bfs_write_buffer_t bfs_write_buffer_t;
//typedef struct bfs_file_t bfs_file_t;
typedef struct bfs_fs_t bfs_fs_t;

//file_impl
/*int32_t bfs_pread(char* buf, int32_t read_size, int64_t offset, bool reada = false);
int32_t bfs_seek(int64_t offset, int32_t whence);
int32_t bfs_read(char* buf, int32_t read_size);
int32_t bfs_write(const char* buf, int32_t write_size);
int32_t bfs_flush();
int32_t sync();
int32_t close();*/

//fs
bfs_fs_t* bfs_open_file_system();
int32_t bfs_create_directory (bfs_fs_t* fs, const char* path);

#ifdef __cplusplus
}/*end extern "C" */
#endif

#pragma GCC visibility pop

#endif  //__BFS_C_H_

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
