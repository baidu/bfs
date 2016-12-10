// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <algorithm>

#include <fuse_lowlevel.h>

#include <sdk/bfs.h>

// not used
// baidu::bfs::FS* g_fs;
std::string g_bfs_path;
std::string g_bfs_cluster;

#define BFS "\e[0;32m[BFS]\e[0m "

static void bfs_ll_getattr(fuse_req_t req, fuse_ino_t ino, 
                           struct fuse_file_info *fi) {
    fprintf(stderr, BFS"%s\n", __func__);
}

static void bfs_ll_lookup(fuse_req_t req, fuse_ino_t parent, const char *name) {
    fprintf(stderr, BFS"%s(%s)\n", __func__, name);
}

static void bfs_ll_readdir(fuse_req_t req, fuse_ino_t ino, 
                           size_t size, off_t off, struct fuse_file_info *fi) {
    fprintf(stderr, BFS"%s\n", __func__);
}

static void bfs_ll_open(fuse_req_t req, fuse_ino_t ino, 
                        struct fuse_file_info *fi) {
    fprintf(stderr, BFS"%s\n", __func__);
}

static void bfs_ll_opendir(fuse_req_t req, fuse_ino_t ino, 
                           struct fuse_file_info *fi) {
    fprintf(stderr, BFS"%s\n", __func__);
}

static void bfs_ll_read(fuse_req_t req, fuse_ino_t ino, 
                        size_t size, off_t off, struct fuse_file_info *fi) {
    fprintf(stderr, BFS"%s(size:%lu, off:%ld)\n", __func__, size, off);
}

static void bfs_ll_write(fuse_req_t req, fuse_ino_t ino, const char *buf, 
                         size_t size, off_t off, struct fuse_file_info *fi) {
    fprintf(stderr, BFS"%s(size:%lu, off:%ld)\n", __func__, size, off);
}

static void bfs_ll_mknod(fuse_req_t req, fuse_ino_t parent, 
                         const char *name, mode_t mode, dev_t dev) {
    fprintf(stderr, BFS"%s(%s)\n", __func__, name);
}

static void bfs_ll_mkdir(fuse_req_t req, fuse_ino_t parent, 
                         const char *name, mode_t mode) {
    fprintf(stderr, BFS"%s(%s)\n", __func__, name);
}

static void bfs_ll_create(fuse_req_t req, fuse_ino_t parent, 
                          const char *name, mode_t mode, 
                          struct fuse_file_info *fi) {
    fprintf(stderr, BFS"%s(%s)\n", __func__, name);
}

static void bfs_ll_statfs(fuse_req_t req, fuse_ino_t ino){
    fprintf(stderr, BFS"%s\n", __func__);
}

static void bfs_ll_rename(fuse_req_t req, fuse_ino_t sparent, 
                          const char *sname, fuse_ino_t tparent, 
                          const char *tname) {
    fprintf(stderr, BFS"%s(source name:%s, target name:%s)\n", __func__, sname, tname);
}

static void bfs_ll_link(fuse_req_t req, fuse_ino_t ino, fuse_ino_t tparent, const char *tname) {
    fprintf(stderr, BFS"%s(target name:%s)\n", __func__, tname);
}

static void bfs_ll_unlink(fuse_req_t req, fuse_ino_t parent, const char *name) {
    fprintf(stderr, BFS"%s(%s)\n", __func__, name);
}

static void bfs_ll_rmdir(fuse_req_t req, fuse_ino_t parent, const char *name) {
    fprintf(stderr, BFS"%s(%s)\n", __func__, name);
}

static void bfs_ll_fsync(fuse_req_t req, fuse_ino_t ino, 
                         int datasync, struct fuse_file_info *fi) {
    fprintf(stderr, BFS"%s\n", __func__);
}

static void bfs_ll_fsyncdir(fuse_req_t req, fuse_ino_t ino, 
                            int datasync, struct fuse_file_info *fi) {
    fprintf(stderr, BFS"%s\n", __func__);
}

static void bfs_ll_access(fuse_req_t req, fuse_ino_t ino, int mask) {
    fprintf(stderr, BFS"%s\n", __func__);
}


int parse_bfs_args(int* argc, char* argv[]) {
    if (*argc < 2) {
        fprintf(stderr, "Usage: %s mount_point [-d]"
                        " [-c bfs_cluster_addr]"
                        " [-p bfs_path]\n",
                argv[0]);
        fprintf(stderr, "\t-d                    Fuse debug (optional)\n"
                        "\t-c bfs_cluster_addr   Ip:port\n"
                        "\t-p bfs_path           The path in BFS which you mount to the mount_point\n"
                        "Example:\n"
                        "       %s /mnt/bfs -d -c 127.0.0.1:8827 -p /\n",
                argv[0]);
        return -1;
    }

    for (int i = 1; i + 1 < *argc; i++) {
        if (strncmp(argv[i], "-c", 2) == 0) {
            g_bfs_cluster = argv[i + 1];
            printf(BFS"Use cluster: %s\n", g_bfs_cluster.c_str());
        } else if (strncmp(argv[i], "-p", 2) == 0) {
            g_bfs_path = argv[i + 1];
            printf(BFS"Use path: %s\n", g_bfs_path.c_str());
        } else {
            continue;
        }
        for (int j = i; j + 2 < *argc; j++) {
            argv[j] = argv[j + 2];
        }
        i -= 2;
        *argc -= 2;
    }
    argv[*argc] = NULL;
    return 0;
}

int main(int argc, char *argv[])
{
    static struct fuse_lowlevel_ops ll_oper = {
        .lookup = bfs_ll_lookup;
        .getattr = bfs_ll_getattr;
        .readdir = bfs_ll_readdir;
        .open = bfs_ll_open;
        .opendir = bfs_ll_opendir;
        .read = bfs_ll_read;
        .write = bfs_ll_write;
        .mknod = bfs_ll_mknod;
        .mkdir = bfs_ll_mkdir;
        .create = bfs_ll_create;
        .statfs = bfs_ll_statfs;
        .rename = bfs_ll_rename;
        .link = bfs_ll_link;
        .unlink = bfs_ll_unlink;
        .rmdir = bfs_ll_rmdir;
        .fsync = bfs_ll_fsync;
        .fsyncdir = bfs_ll_fsyncdir;
        .access = bfs_ll_access;
    };

    if (parse_bfs_args(&argc, argv) != 0) {
        return -1;
    }

    struct fuse_args args = FUSE_ARGS_INIT(argc, argv);
    char *mountpoint = NULL;

    int err = 0;
    if ((err = fuse_parse_cmdline(&args, &mountpoint, NULL, NULL)) != 0) {
        fprintf(stderr, "fuse parse command line arguments error ret=%d\n", err);
        return -1;
    }

    struct fuse_chan* ch = fuse_mount(mountpoint, &args);
    if (ch == NULL) {
        fprintf(stderr, "fuse mount error\n");
        return -1;
    }

    struct fuse_session* se = 
        fuse_lowlevel_new(&args, &ll_oper, sizeof(ll_oper), NULL);
    if (se == NULL) {
        fprintf(stderr, "fuse lowlevel new session error\n");
        return -1;
    }

    if ((err = fuse_set_signal_handlers(se)) != 0) {
        fprintf(stderr, "fuse set signal handlers error ret=%d\n", err);
        return -1;
    }

    fprintf(stderr, BFS"fuse_set_signal_handlers success.");
    fuse_session_add_chan(se, ch);

    err = fuse_session_loop(se);

    fuse_remove_signal_handlers(se);
    fuse_session_remove_chan(ch);
    fuse_session_destroy(se);
    fuse_unmount(mountpoint, ch);
    fuse_opt_free_args(&args);

    return err;
}
