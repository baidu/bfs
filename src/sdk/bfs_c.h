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

typedef struct bfs_fs_t bfs_fs_t;

bfs_fs_t* bfs_open_file_system();
int bfs_create_directory(bfs_fs_t* fs, const char* path);
int bfs_list_directory(bfs_fs_t* fs, const char* path);
int bfs_delete_file(bfs_fs_t* fs, const char* path);
int bfs_rename(bfs_fs_t* fs, const char* oldpath, const char* newpath);
int bfs_touchz(bfs_fs_t* fs, const char* path);
int bfs_symlink(bfs_fs_t* fs, const char* src, const char* dst);
int bfs_cat(bfs_fs_t* fs, const char* path);
int bfs_get(bfs_fs_t* fs, const char* src, const char* tgt);
int bfs_put(bfs_fs_t* fs, const char* local, const char* bfs);
int64_t bfs_du_v2(bfs_fs_t* fs, const char* path);
int bfs_du(bfs_fs_t* fs, const char* path);
int bfs_rm_dir(bfs_fs_t* fs,  const char* path, bool recursive);
int bfs_change_replica_num(bfs_fs_t* fs,  const char* path, const char* replica_num);
int bfs_chmod(bfs_fs_t* fs, const char* str_mode, const char* path);
int bfs_location(bfs_fs_t* fs, const char* path);

#ifdef __cplusplus
}/*end extern "C" */
#endif

#pragma GCC visibility pop

#endif  //__BFS_C_H_

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
