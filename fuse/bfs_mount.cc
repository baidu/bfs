// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <fuse.h>
#include <errno.h>
#include <fcntl.h>
#include <algorithm>
#include <sdk/bfs.h>

baidu::bfs::FS* g_fs;
std::string g_bfs_path;
std::string g_bfs_cluster;

#define BFS "\e[0;32m[BFS]\e[0m "

struct MountFile {
    baidu::bfs::File* bfs_file;
    int64_t offset;
    char* buf;
    int32_t buf_len;
    MountFile(baidu::bfs::File* bfile) : bfs_file(bfile), offset(0), buf(NULL), buf_len(0) {}
};

baidu::bfs::File* get_bfs_file(const struct fuse_file_info* finfo, MountFile** mount_file = NULL) {
    MountFile* mfile = reinterpret_cast<MountFile*>(finfo->fh);
    if (mount_file) *mount_file= mfile;
    return mfile->bfs_file;
}

int bfs_getattr(const char* path, struct stat* st) {
    fprintf(stderr, BFS"bfs_getattr(%s)\n", path);
    baidu::bfs::BfsFileInfo file;
    int32_t ret = g_fs->Stat((g_bfs_path + path).c_str(), &file);
    if (ret != OK) {
        fprintf(stderr, BFS"stat %s fail, error code:%s\n",
                path, baidu::bfs::StrError(ret));
        return -ENOENT;
    }
    memset(st, 0, sizeof(struct stat));
    if (file.mode & (01000)) {
        st->st_mode = (file.mode & 0777) | S_IFDIR;
        st->st_size = 4096;
    } else {
        st->st_mode = (file.mode & 0777) | S_IFREG;
        if (file.size == 0) {
            int64_t file_size = 0;
            if (g_fs->GetFileSize((g_bfs_path + path).c_str(), &file_size) == OK
                    && file_size > 0) {
                st->st_size = file_size;
            } else {
                st->st_size = 0;
            }
        } else {
            st->st_size = file.size;
        }
    }
    st->st_blocks = 1;
    fprintf(stderr, BFS"bfs_getattr(%s) ctime=%u size=%ld\n", path, file.ctime, st->st_size);
    st->st_atime = file.ctime;
    st->st_ctime = file.ctime;
    st->st_mtime = file.ctime;
    st->st_nlink = 1;
    return 0;
}

int bfs_readlink(const char* path, char* , size_t) {
    fprintf(stderr, BFS"readlink(%s)\n", path);
    return EINVAL;
}

int bfs_mknod(const char* path, mode_t, dev_t) {
    fprintf(stderr, BFS"mknode(%s)\n", path);
    return EPERM;
}

int bfs_mkdir(const char *path, mode_t) {
    fprintf(stderr, BFS"mkdir(%s)\n", path);
    int32_t ret = g_fs->CreateDirectory((g_bfs_path+path).c_str());
    if (ret != OK) {
        fprintf(stderr, BFS"mkdir %s fail, error code %s\n",
                path, baidu::bfs::StrError(ret));
        return EACCES;
    }
    return 0;
}

int bfs_ulink(const char* path) {
    fprintf(stderr, BFS"unlink(%s)\n", path);
    int32_t ret = g_fs->DeleteFile((g_bfs_path + path).c_str());
    if (ret != OK) {
        fprintf(stderr, BFS"unlink %s fail, error code %s\n",
                path, baidu::bfs::StrError(ret));
        return EACCES;
    }
    return 0;
}

int bfs_rmdir(const char* path) {
    fprintf(stderr, BFS"unlink(%s)\n", path);
    int32_t ret = g_fs->DeleteDirectory((g_bfs_path + path).c_str(), true);
    if (ret != OK) {
        fprintf(stderr, BFS"unlink %s fail, error code %s\n",
                path, baidu::bfs::StrError(ret));
        return EACCES;
    }
    return 0;
}

int bfs_symlink(const char* oldpath, const char* newpath) {
    fprintf(stderr, BFS"symlink(%s, %s)\n", oldpath, newpath);
    return EPERM;
}

int bfs_rename(const char* oldpath, const char* newpath) {
    fprintf(stderr, BFS"Rename(%s, %s)\n", oldpath, newpath);
    int32_t ret = g_fs->Rename((g_bfs_path + oldpath).c_str(), (g_bfs_path + newpath).c_str());
    if (ret != OK) {
        fprintf(stderr, BFS"Rename %s to %s fail, error code %s\n",
                oldpath, newpath, baidu::bfs::StrError(ret));
        return EACCES;
    }
    return 0;
}

int bfs_link(const char* oldpath, const char* newpath) {
    fprintf(stderr, BFS"link(%s, %s)\n", oldpath, newpath);
    return EPERM;
}

int bfs_chmod(const char* name, mode_t mode) {
    fprintf(stderr, BFS"chmod(%s, %d)\n", name, mode);
    return 0;
}

int bfs_chown(const char* name, uid_t, gid_t) {
    fprintf(stderr, BFS"chown(%s)\n", name);
    return 0;
}

int bfs_truncate(const char* name, off_t offset) {
    fprintf(stderr, BFS"truncate %s %ld\n", name, offset);
    return EPERM;
}

void prepare_write_buf(MountFile* mfile, baidu::bfs::File* file = NULL) {
    const int init_write_buf_size = 4096;
    mfile->buf = new char[init_write_buf_size];
    memset(mfile->buf, 0, init_write_buf_size);
    mfile->buf_len = init_write_buf_size;
}
int bfs_open(const char* path, struct fuse_file_info* finfo) {
    fprintf(stderr, BFS"open(%s, %o)\n", path, finfo->flags);
    baidu::bfs::File* file = NULL;
    int32_t ret = g_fs->OpenFile((g_bfs_path + path).c_str(), O_RDONLY,
                                    &file, baidu::bfs::ReadOptions());
    if (ret != OK) {
        fprintf(stderr, BFS"open(%s) fail, error code %s\n",
                path, baidu::bfs::StrError(ret));
        return EACCES;
    }
    fprintf(stderr, BFS"open(%s) return %p\n", path, file);
    MountFile* mfile = new MountFile(file);
    if (finfo->flags & O_RDWR) {
        prepare_write_buf(mfile, file);
    }
    finfo->fh = reinterpret_cast<uint64_t>(mfile);
    return 0;
}

int bfs_read(const char* path, char* buf, size_t len, off_t offset, struct fuse_file_info* finfo) {
    fprintf(stderr, BFS"read(%s, %ld, %lu)\n", path, offset, len);
    baidu::bfs::File* file = get_bfs_file(finfo);
    int ret = file->Pread(buf, len, offset, true);
    fprintf(stderr, BFS"read(%s, %ld, %lu) return %d\n", path, offset, len, ret);
    if (ret < 0) {
        ret = EACCES;
    }
    return ret;
}

int bfs_write(const char* path, const char* buf, size_t len, off_t offset, struct fuse_file_info* finfo) {
    const int zero_buf_size = 256 * 1024;
    const int max_random_write_size = 256 * 1024 * 1024;
    static char zero_buf[zero_buf_size] = {0};
    fprintf(stderr, BFS"write(%s, %ld, %lu)\n", path, offset, len);
    MountFile* mfile = NULL;
    baidu::bfs::File* file = get_bfs_file(finfo, &mfile);
    if (mfile->buf || mfile->offset > offset) {
        fprintf(stderr, BFS"random write(%s, %ld, %lu) old offset= %ld\n", path, offset, len, mfile->offset);
        int64_t end_offset = offset + len;
        if (end_offset > max_random_write_size || mfile->offset > max_random_write_size) {
            return EACCES;
        }
        int new_buf_len = std::min(static_cast<int64_t>(max_random_write_size), std::max(mfile->offset, end_offset * 2));
        if (mfile->buf == NULL) {
            mfile->buf = new char[new_buf_len];
            mfile->buf_len = new_buf_len;
            int rlen = file->Pread(mfile->buf, mfile->offset, 0);
            if (rlen < mfile->offset) {
                fprintf(stderr, BFS"Read(%ld) for randmon write(%s, %ld, %lu) fail", mfile->offset, path, offset, len);
                delete[] mfile->buf;
                mfile->buf = NULL;
                mfile->buf_len = 0;
                return EACCES;
            }
        } else if (mfile->buf_len < end_offset) {
            char* new_buf = new char[new_buf_len];
            memcpy(new_buf, mfile->buf, mfile->buf_len);
            delete[] mfile->buf;
            mfile->buf = new_buf;
            mfile->buf_len = new_buf_len;
        }
        memcpy(mfile->buf + offset, buf, len);
        return len;
    } else if (mfile->offset < offset) {
        if (mfile)
        // Padding if skip
        fprintf(stderr, BFS"Write(%s, %ld, %lu) padding from %ld\n", path, offset, len, mfile->offset);
        while (mfile->offset < offset) {
            int blen = std::min(static_cast<int64_t>(zero_buf_size), offset - mfile->offset);
            int wlen = file->Write(zero_buf, blen);
            if (wlen > 0) {
                mfile->offset += wlen;
            }
            if (wlen < blen) {
                fprintf(stderr, BFS"Write(%s, %ld, %lu) padding at %ld fail w:%d b:%d\n",
                        path, offset, len, mfile->offset, wlen, blen);
                return EACCES;
            }
        }
    }
    int ret = file->Write(buf, len);
    if (ret > 0) {
        mfile->offset += ret;
    }
    fprintf(stderr, BFS"write(%s, %ld, %lu) return %d\n", path, offset, len, ret);
    if (ret < 0) {
        ret = EACCES;
    }
    return ret;
}

int bfs_statfs(const char* path, struct statvfs*) {
    fprintf(stderr, BFS"statfs(%s)\n", path);
    return 0;
}

int bfs_flush(const char* path, struct fuse_file_info* finfo) {
    baidu::bfs::File* file = get_bfs_file(finfo);
    fprintf(stderr, BFS"flush(%s, %p)\n", path, file);
    int32_t ret = file->Sync();
    if (ret != OK) {
        fprintf(stderr, BFS"fsync(%s, %p) fail, error code %s\n",
                path, file, baidu::bfs::StrError(ret));
        return EIO;
    }
    fprintf(stderr, BFS"flush(%s, %p) return 0\n", path, file);
    return 0;
}

int bfs_release(const char* path, struct fuse_file_info* finfo) {
    MountFile* mfile = NULL;
    baidu::bfs::File* file = get_bfs_file(finfo, &mfile);
    int32_t ret = file->Close();
    fprintf(stderr, BFS"release(%s, %p, %d)\n", path, file, ret);
    delete file;

    int retval = 0;
    if (mfile->buf) {
        g_fs->DeleteFile(path);
        int32_t ret = g_fs->OpenFile((g_bfs_path + path).c_str() , O_WRONLY,
                                    0755, &file, baidu::bfs::WriteOptions());
        if (ret != OK) {
            fprintf(stderr, BFS"create(%s) for release fail, error code %s\n",
                    path, baidu::bfs::StrError(ret));
            retval = EACCES;
        }
        int wlen = file->Write(mfile->buf, mfile->buf_len);
        if (wlen < mfile->buf_len) {
            fprintf(stderr, BFS"Write(%s, %d) for release fail\n", path, mfile->buf_len);
            retval = EACCES;
        }
        file->Close();
        delete[] mfile->buf;
    }
    delete mfile;
    return retval;
}

int bfs_fsync(const char* path, int datasync, struct fuse_file_info* finfo) {
    baidu::bfs::File* file = get_bfs_file(finfo);
    fprintf(stderr, BFS"fsync(%s, %p)\n", path, file);
    int32_t ret = file->Sync();
    if (ret != OK) {
        fprintf(stderr, BFS"fsync(%s, %p) fail, error code\n",
                path, file, baidu::bfs::StrError(ret));
        return EIO;
    }
    fprintf(stderr, BFS"fsync(%s, %p) return 0\n", path, file);
    return 0;
}

/** Set extended attributes */
int (*setxattr) (const char *, const char *, const char *, size_t, int);

/** Get extended attributes */
int (*getxattr) (const char *, const char *, char *, size_t);
int bfs_getxattr(const char * path, const char* key, char *, size_t) {
    fprintf(stderr, BFS"bfs_getxattr(%s, %s)\n", path, key);
    return 0;
}

/** List extended attributes */
int (*listxattr) (const char *, char *, size_t);

/** Remove extended attributes */
int (*removexattr) (const char *, const char *);

int bfs_opendir(const char* path, struct fuse_file_info *) {
    fprintf(stderr, BFS"opendir(%s)\n", path);
    return 0;
}

int bfs_readdir(const char* path, void* buf, fuse_fill_dir_t filler,
                              off_t offset, struct fuse_file_info* fi) {
    //return filler(buf, "hello-world", NULL, 0);
    fprintf(stderr, BFS"readdir(%s)\n", path);
    baidu::bfs::BfsFileInfo* files = NULL;
    int num = 0;
    g_fs->ListDirectory((g_bfs_path + path).c_str(), &files, &num);
    for (int i = 0; i < num; i++) {
        struct stat file_stat;
        memset(&file_stat, 0 ,sizeof(file_stat));
        file_stat.st_size = files[i].size;
        if (files[i].mode & (1<<9)) {
            file_stat.st_mode = (files[i].mode&0777) | S_IFDIR;
        } else {
            file_stat.st_mode = (files[i].mode&0777) | S_IFREG;
        }
        file_stat.st_atime = files[i].ctime;
        file_stat.st_ctime = files[i].ctime;
        file_stat.st_mtime = files[i].ctime;
 
        int ret = filler(buf, files[i].name, &file_stat, 0);
        assert(ret == 0);
    }
    delete[] files;
    return 0;
}

int bfs_releasedir(const char* path, struct fuse_file_info *) {
    fprintf(stderr, BFS"readdir(%s)\n", path);
    return 0;
}

int bfs_fsyncdir(const char* path, int, struct fuse_file_info*) {
    fprintf(stderr, BFS"fsyncdir(%s)\n", path);
    return 0;
}

int bfs_access(const char* path, int mode) {
    fprintf(stderr, BFS"access(%s, %d)\n", path, mode);
    return 0;
}

/**
 * Create and open a file
 *
 * If the file does not exist, first create it with the specified
 * mode, and then open it.
 *
 * If this method is not implemented or under Linux kernel
 * versions earlier than 2.6.15, the mknod() and open() methods
 * will be called instead.
 *
 * Introduced in version 2.5
 */
int (*create) (const char *, mode_t, struct fuse_file_info *);
int bfs_create(const char* path, mode_t mode, struct fuse_file_info* finfo) {
    fprintf(stderr, BFS"create(%s, %o, %o)\n", path, mode, finfo->flags);
    baidu::bfs::File* file = NULL;
    int32_t ret = g_fs->OpenFile((g_bfs_path + path).c_str() , O_WRONLY, mode,
                                 &file, baidu::bfs::WriteOptions());
    if (ret != OK) {
        fprintf(stderr, BFS"create(%s) fail, error code %s\n",
                path, baidu::bfs::StrError(ret));
        return EACCES;
    }
    fprintf(stderr, BFS"create(%s) return %p\n", path, file);
    MountFile* mfile = new MountFile(file);
    finfo->fh = reinterpret_cast<uint64_t>(mfile);
    return 0;
}

int bfs_ftruncate(const char* path, off_t offset, struct fuse_file_info*) {
    fprintf(stderr, BFS"ftruncate(%s, %ld)\n", path, offset);
    return 0;
}

int bfs_fgetattr(const char* path, struct stat* st, struct fuse_file_info*) {
    fprintf(stderr, BFS"fgetattr(%s)\n", path);
    return bfs_getattr(path, st);
}

int bfs_lock(const char* path, struct fuse_file_info *, int cmd, struct flock *) {
    fprintf(stderr, BFS"lock(%s, %d)\n", path, cmd);
    return 0;
}

int bfs_utimens(const char* path, const struct timespec tv[2]) {
    fprintf(stderr, BFS"utimes(%s)\n", path);
    return 0;
}

int bfs_bmap(const char* path, size_t blocksize, uint64_t *idx) {
    fprintf(stderr, BFS"bmap(%s)\n", path);
    return 0;
}

void* bfs_init(struct fuse_conn_info *conn) {
    fprintf(stderr, BFS"init()\n");
    if (g_bfs_cluster.empty()) {
        //g_bfs_cluster = "yq01-tera60.yq01:8828";
        g_bfs_cluster = "localhost:8828";
    }
    if (!baidu::bfs::FS::OpenFileSystem(g_bfs_cluster.c_str(), &g_fs, baidu::bfs::FSOptions())) {
        fprintf(stderr, BFS"Open file sytem: %s fail\n", g_bfs_cluster.c_str());
        abort();
    }
    int32_t ret = g_fs->Access(g_bfs_path.c_str(), R_OK | W_OK);
    if (ret != OK) {
        fprintf(stderr, BFS"Access %s fail, error code %s\n",
                g_bfs_path.c_str(), baidu::bfs::StrError(ret));
        abort();
    }
    return g_fs;
}

void bfs_destroy(void*) {
    fprintf(stderr, BFS"destroy()\n");
}

int parse_args(int* argc, char* argv[]) {
    if (*argc < 2) {
        fprintf(stderr, "usage %s mount_point [-d]"
                        " [-c bfs_cluster_addr]"
                        " [-p bfs_path]\n",
                argv[0]);
        return 1;
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

int main(int argc, char* argv[]) {
    if (parse_args(&argc, argv)) {
        return 1;
    }

    static struct fuse_operations ops;
    ops.getattr = bfs_getattr,
    ops.readlink = bfs_readlink,
    ops.mknod = bfs_mknod;
    ops.mkdir = bfs_mkdir;
    ops.unlink = bfs_ulink;
    ops.rmdir = bfs_rmdir;
    ops.symlink = bfs_symlink;
    ops.rename = bfs_rename;
    ops.link = bfs_link;
    ops.chmod = bfs_chmod;
    ops.chown = bfs_chown;
    ops.truncate = bfs_truncate;
    ops.open = bfs_open;
    ops.read = bfs_read;
    ops.write = bfs_write;
    ops.statfs = bfs_statfs;
    ops.flush = bfs_flush;
    ops.release = bfs_release;
    ops.fsync = bfs_fsync;
    ops.opendir = bfs_opendir;
    ops.readdir = bfs_readdir;
    ops.getxattr = bfs_getxattr;
    ops.releasedir = bfs_releasedir;
    ops.fsyncdir = bfs_fsyncdir;
    ops.init = bfs_init;
    ops.destroy = bfs_destroy;
    ops.access = bfs_access;
    ops.create = bfs_create;
    ops.ftruncate = bfs_ftruncate;
    ops.fgetattr = bfs_fgetattr;
    ops.lock = bfs_lock;
    ops.utimens = bfs_utimens;
    ops.bmap = bfs_bmap;

    return fuse_main(argc, argv, &ops, NULL);
}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
