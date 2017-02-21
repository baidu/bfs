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

baidu::bfs::FS* g_fs;
std::string g_bfs_path;
std::string g_bfs_cluster;

#define BFS "\e[0;32m[BFS]\e[0m "
#define min(x, y) ((x) < (y) ? (x) : (y))
struct MountFile {
    baidu::bfs::File* bfs_file;
    bool read_only;
    char* buf;
    int32_t buf_len;
    int64_t file_size;
    std::string file_path;
    MountFile(baidu::bfs::File* bfile, const std::string& path)
        : bfs_file(bfile), read_only(false),
          buf(NULL), buf_len(0),
          file_size(0), file_path(path) {}
};

baidu::bfs::File* get_bfs_file(const struct fuse_file_info* finfo,
                                MountFile** mount_file = NULL) {
    MountFile* mfile = reinterpret_cast<MountFile*>(finfo->fh);
    if (mount_file) *mount_file = mfile;
    return mfile->bfs_file;
}

static int bfsfileinfo_to_stat(baidu::bfs::BfsFileInfo *bfile, struct stat *st) {
    
    if (bfile->mode & (01000)) {
        st->st_mode = (bfile->mode & 0777) | S_IFDIR;
        st->st_size = 4096;
    } else {
        st->st_mode = (bfile->mode & 0777) | S_IFREG;
        st->st_size = bfile->size;
    }
    st->st_ino = bfile->ino;
    st->st_atime = bfile->ctime;
    st->st_ctime = bfile->ctime;
    st->st_mtime = bfile->ctime;
    /*The correct value of st_nlink for directories is NSUB + 2.  Where NSUB
    is the number of subdirectories.  NOTE: regular-file/symlink/etc
    entries do not count into NSUB, only directories.
    
    If calculating NSUB is hard, the filesystem can set st_nlink of
    directories to 1, and find will still work.  This is not documented
    behavior of find, and it's not clear whether this is intended or just
    by accident.  But for example the NTFS filesysem relies on this, so
    it's unlikely that this "feature" will go away.
    */
    st->st_nlink = 1; 
    return 0;
}

static int bfs_getattr(fuse_ino_t ino, struct stat *st) {
    fprintf(stderr, BFS"%s\n", __func__);
    baidu::bfs::BfsFileInfo bfile;
    int32_t ret = g_fs->IGet(ino, &bfile);
    if (ret != baidu::bfs::OK) {
        fprintf(stderr, BFS"%s IGet ino:%lu fail!\n", __func__, ino);
        return -1;
    }
    std::string file_path(bfile.name);
    ret = g_fs->Stat(file_path.c_str(), &bfile);
    if (ret != baidu::bfs::OK) {
        fprintf(stderr, BFS"%s Stat file_path:%s fail!\n", __func__, file_path.c_str());
        return -1;
    }
    ret = bfsfileinfo_to_stat(&bfile, st);
    if (ret != baidu::bfs::OK) {
        fprintf(stderr, BFS"%s bfsfileinfo_to_stat fail!\n", __func__);
        return -1;
    }
    return 0;
}

static int bfs_lookup(fuse_ino_t parent, const char *name, baidu::bfs::BfsFileInfo *bfile) {
    fprintf(stderr, BFS"%s\n", __func__);
    baidu::bfs::BfsFileInfo fileinfo;
    int32_t ret = g_fs->IGet(parent, &fileinfo);
    if (ret != baidu::bfs::OK) {
        fprintf(stderr, BFS"%s IGet ino:%lu fail!\n", __func__, parent);
        return -1;
    }
    std::string file_path(fileinfo.name);
    file_path.append("/");
    file_path.append(name);
    ret = g_fs->Stat(file_path.c_str(), bfile);
    if (ret != baidu::bfs::OK) {
        fprintf(stderr, BFS"%s Stat file_path:%s fail!\n", __func__, file_path.c_str());
        return -1;
    }
    return 0;
}

static void bfs_ll_getattr(fuse_req_t req, fuse_ino_t ino, 
                           struct fuse_file_info *fi) {
    fprintf(stderr, BFS"%s\n", __func__);
    struct stat st;
    memset(&st, 0, sizeof(struct stat));
    if (bfs_getattr(ino, &st) == -1) {
        fuse_reply_err(req, ENOENT);
    } else {
        fuse_reply_attr(req, &st, 1.0);
    }
}

static void bfs_ll_lookup(fuse_req_t req, fuse_ino_t parent, const char *name) {
    fprintf(stderr, BFS"%s parent_ino:%lu name:%s\n", __func__, parent, name);
    struct fuse_entry_param e;
    baidu::bfs::BfsFileInfo bfile;

    int32_t ret = bfs_lookup(parent, name, &bfile);
    if (ret != baidu::bfs::OK) {
        fprintf(stderr, BFS"%s bfs_lookup ino:%lu name:%s fail!\n", __func__, parent, name);
        fuse_reply_err(req, ENOENT);
    } else {
        e.ino = bfile.ino;
        e.attr_timeout = 1.0;
        e.entry_timeout = 1.0;
        ret = bfsfileinfo_to_stat(&bfile, &e.attr);
        if (ret != baidu::bfs::OK) {
            fprintf(stderr, BFS"%s bfsfileinfo_to_stat fail!\n", __func__);
            fuse_reply_err(req, ENOENT);
        }
        fuse_reply_entry(req, &e);
    }
}

struct dirbuf {
    char *p;
    uint32_t size_used;
};

static void dirbuf_add(fuse_req_t req, struct dirbuf *b, const char *name,
                        fuse_ino_t ino) {
    struct stat st;
    size_t oldsize = b->size_used;
    b->size_used += fuse_add_direntry(req, NULL, 0, name, NULL, 0);
    b->p = (char *) realloc(b->p, b->size_used);
    memset(&st, 0, sizeof(st));
    st.st_ino = ino;
    fuse_add_direntry(req, b->p + oldsize, b->size_used - oldsize, name, &st,
                        b->size_used);
}
 
static int reply_buf_limited(fuse_req_t req, const char *buf, size_t bufsize,
                  off_t off, size_t maxsize) {
    if (bufsize - off > 0) {
        return fuse_reply_buf(req, buf + off, min(bufsize - off, maxsize));
    } else {
        return fuse_reply_buf(req, NULL, 0);
    }
}

static void bfs_ll_readdir(fuse_req_t req, fuse_ino_t ino, 
                           size_t size, off_t off, struct fuse_file_info *fi) {
    fprintf(stderr, BFS"%s\n", __func__);
    struct stat st;
    struct dirbuf b;
    int32_t dir_num = 0;
    baidu::bfs::BfsFileInfo bfile;
    baidu::bfs::BfsFileInfo* bfiles = NULL;

    int32_t ret = g_fs->IGet(ino, &bfile);
    if (ret != baidu::bfs::OK) {
        fprintf(stderr, BFS"%s IGet ino:%lu fail!\n", __func__, ino);
        fuse_reply_err(req, ENOTDIR);
        return ;
    }
    std::string file_path(bfile.name);
    ret = g_fs->ListDirectory(file_path.c_str(), &bfiles, &dir_num);
    if (ret != baidu::bfs::OK) {
        fprintf(stderr, BFS"%s ListDirectory file_path:%s fail!\n", __func__, file_path.c_str());
        fuse_reply_err(req, ENOTDIR);
        return ;
    }
    memset(&st, 0, sizeof(st));
    memset(&b, 0, sizeof(b));
    for (int i = 0; i < dir_num; i++) {
        ret = bfsfileinfo_to_stat(&bfiles[i], &st);
        if (ret != baidu::bfs::OK) {
            fprintf(stderr, BFS"%s bfsfileinfo_to_stat fail!\n", __func__);
            fuse_reply_err(req, ENOTDIR);
            return ;
        }
        dirbuf_add(req, &b, bfiles[i].name, bfiles[i].ino);
    }
    reply_buf_limited(req, b.p, b.size_used, off, size);
    delete[] bfiles;
    free(b.p);
}

static void bfs_ll_open(fuse_req_t req, fuse_ino_t ino, 
                        struct fuse_file_info *fi) {
    fprintf(stderr, BFS"%s\n", __func__);
}

static void bfs_ll_opendir(fuse_req_t req, fuse_ino_t ino, 
                           struct fuse_file_info *fi) {
    fprintf(stderr, BFS"%s\n", __func__);
    fuse_reply_open(req, fi);
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
    struct fuse_entry_param e;
    baidu::bfs::BfsFileInfo bfile;

    int32_t ret = g_fs->IGet(parent, &bfile);
    if (ret != baidu::bfs::OK) {
        fprintf(stderr, BFS"%s IGet ino:%lu fail!\n", __func__, parent);
        fuse_reply_err(req, ENOENT);
        return ;
    }
    std::string file_path(bfile.name);
    file_path.append("/");
    file_path.append(name);
    ret = g_fs->CreateDirectory(file_path.c_str());
    if (ret != baidu::bfs::OK) {
        fprintf(stderr, BFS"%s CreateDirectory file_path:%s fail!\n", __func__, file_path.c_str());
        fuse_reply_err(req, ENOENT);
        return ;
    }
    ret = g_fs->Stat(file_path.c_str(), &bfile);
    if (ret != baidu::bfs::OK) {
        fprintf(stderr, BFS"%s Stat file_path:%s fail!\n", __func__, file_path.c_str());
        fuse_reply_err(req, ENOENT);
    } else {
        e.ino = bfile.ino;
        e.attr_timeout = 1.0;
        e.entry_timeout = 1.0;
        ret = bfsfileinfo_to_stat(&bfile, &e.attr);
        if (ret != baidu::bfs::OK) {
            fprintf(stderr, BFS"%s bfsfileinfo_to_stat fail!\n", __func__);
            fuse_reply_err(req, ENOENT);
        }
        fuse_reply_entry(req, &e);
    }
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
    baidu::bfs::BfsFileInfo bfile;

    int32_t ret = g_fs->IGet(parent, &bfile);
    if (ret != baidu::bfs::OK) {
        fprintf(stderr, BFS"%s IGet ino:%lu fail!\n", __func__, parent);
        fuse_reply_err(req, ENOENT);
        return ;
    }
    std::string file_path(bfile.name);
    file_path.append("/");
    file_path.append(name);
    ret = g_fs->DeleteDirectory(file_path.c_str(), true);
    if (ret != baidu::bfs::OK) {
        fprintf(stderr, BFS"%s DeleteDirectory file_path:%s fail!\n", __func__, file_path.c_str());
        fuse_reply_err(req, ENOENT);
        return ;
    }
    fuse_reply_err(req, 0);
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
    fuse_reply_err(req, 0);
}

static void bfs_ll_init(void *userdata, struct fuse_conn_info *conn) {
    fprintf(stderr, BFS"init()\n");
    if (g_bfs_cluster.empty()) {
        g_bfs_cluster = "localhost:8828";
    }
    if (!baidu::bfs::FS::OpenFileSystem(g_bfs_cluster.c_str(), &g_fs, baidu::bfs::FSOptions())) {
        fprintf(stderr, BFS"Open file sytem: %s fail\n", g_bfs_cluster.c_str());
        abort();
    }
    int32_t ret = g_fs->Access(g_bfs_path.c_str(), R_OK | W_OK);
    if (ret != baidu::bfs::OK) {
        fprintf(stderr, BFS"Access %s fail, error code %s\n",
                g_bfs_path.c_str(), baidu::bfs::StrError(ret));
        abort();
    }
    return ;
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
    static struct fuse_lowlevel_ops ll_oper; 
       ll_oper.lookup = bfs_ll_lookup;
       ll_oper.getattr = bfs_ll_getattr;
       ll_oper.readdir = bfs_ll_readdir;
       ll_oper.open = bfs_ll_open;
       ll_oper.opendir = bfs_ll_opendir;
       ll_oper.read = bfs_ll_read;
       ll_oper.write = bfs_ll_write;
       ll_oper.mknod = bfs_ll_mknod;
       ll_oper.mkdir = bfs_ll_mkdir;
       ll_oper.create = bfs_ll_create;
       ll_oper.statfs = bfs_ll_statfs;
       ll_oper.rename = bfs_ll_rename;
       ll_oper.link = bfs_ll_link;
       ll_oper.unlink = bfs_ll_unlink;
       ll_oper.rmdir = bfs_ll_rmdir;
       ll_oper.fsync = bfs_ll_fsync;
       ll_oper.fsyncdir = bfs_ll_fsyncdir;
       ll_oper.access = bfs_ll_access;
       ll_oper.init = bfs_ll_init;

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
