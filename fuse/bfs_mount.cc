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

#include <common/logging.h>

#include <sdk/bfs.h>

baidu::bfs::FS* g_fs;
std::string g_bfs_path;
std::string g_bfs_cluster;

namespace baidu {
namespace bfs {

const int g_max_random_write_size = 256 * 1024 * 1024;

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

baidu::bfs::File* get_bfs_file(const struct fuse_file_info* finfo, MountFile** mount_file = NULL) {
    MountFile* mfile = reinterpret_cast<MountFile*>(finfo->fh);
    if (mount_file) *mount_file= mfile;
    return mfile->bfs_file;
}

int bfs_getattr(const char* path, struct stat* st) {
    LOG(DEBUG, "bfs_getattr(%s)", path);
    baidu::bfs::BfsFileInfo file;
    int32_t ret = g_fs->Stat((g_bfs_path + path).c_str(), &file);
    if (ret != baidu::bfs::OK) {
        LOG(WARNING, "stat %s fail, errcode: %s",
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
            if (g_fs->GetFileSize((g_bfs_path + path).c_str(), &file_size) == baidu::bfs::OK
                    && file_size > 0) {
                st->st_size = file_size;
            } else {
                st->st_size = 0;
            }
        } else {
            st->st_size = file.size;
        }
    }
    st->st_blocks = (st->st_size - 1) / 512 + 1;
    st->st_atime = file.ctime;
    st->st_ctime = file.ctime;
    st->st_mtime = file.ctime;
    st->st_nlink = 1;
    LOG(DEBUG, "bfs_getattr(%s) ctime=%u size=%ld",
            path, file.ctime, st->st_size);
    return 0;
}

int bfs_readlink(const char* path, char* , size_t) {
    LOG(WARNING, "readlink(%s)", path);
    return EINVAL;
}

int bfs_mknod(const char* path, mode_t, dev_t) {
    LOG(DEBUG, "mknode(%s)", path);
    return EPERM;
}

int bfs_mkdir(const char *path, mode_t) {
    LOG(DEBUG, "mkdir(%s)", path);
    int32_t ret = g_fs->CreateDirectory((g_bfs_path+path).c_str());
    if (ret != baidu::bfs::OK) {
        LOG(WARNING, "mkdir %s fail, error code %s",
                path, baidu::bfs::StrError(ret));
        return EACCES;
    }
    return 0;
}

int bfs_ulink(const char* path) {
    LOG(DEBUG, "unlink(%s)", path);
    int32_t ret = g_fs->DeleteFile((g_bfs_path + path).c_str());
    if (ret != baidu::bfs::OK) {
        LOG(WARNING, "unlink %s fail, error code %s",
                path, baidu::bfs::StrError(ret));
        return EACCES;
    }
    return 0;
}

int bfs_rmdir(const char* path) {
    LOG(DEBUG, "unlink(%s)", path);
    int32_t ret = g_fs->DeleteDirectory((g_bfs_path + path).c_str(), true);
    if (ret != baidu::bfs::OK) {
        LOG(WARNING, "unlink %s fail, error code %s",
                path, baidu::bfs::StrError(ret));
        return EACCES;
    }
    return 0;
}

int bfs_symlink(const char* oldpath, const char* newpath) {
    LOG(WARNING, "symlink(%s, %s", oldpath, newpath);
    return EPERM;
}

int bfs_rename(const char* oldpath, const char* newpath) {
    LOG(DEBUG, "Rename(%s, %s", oldpath, newpath);
    int32_t ret = g_fs->Rename((g_bfs_path + oldpath).c_str(), (g_bfs_path + newpath).c_str());
    if (ret != baidu::bfs::OK) {
        LOG(WARNING, "Rename %s to %s fail, error code %s",
                oldpath, newpath, baidu::bfs::StrError(ret));
        return EACCES;
    }
    return 0;
}

int bfs_link(const char* oldpath, const char* newpath) {
    LOG(WARNING, "link(%s, %s)", oldpath, newpath);
    return EPERM;
}

int bfs_chmod(const char* name, mode_t mode) {
    LOG(WARNING, "chmod(%s, %d)", name, mode);
    return 0;
}

int bfs_chown(const char* name, uid_t, gid_t) {
    LOG(WARNING, "chmod(%s)", name);
    return 0;
}

int bfs_truncate(const char* name, off_t offset) {
    LOG(WARNING, "truncate %s %ld", name, offset);
    return EPERM;
}

bool prepare_for_random_write(MountFile* mfile, bool write_only) {
    baidu::bfs::File*& bfs_file = mfile->bfs_file;
    int64_t fsize = 0;
    int ret = g_fs->GetFileSize(mfile->file_path.c_str(), &fsize);
    if (ret != baidu::bfs::OK) {
        LOG(WARNING, "GetFileSize for random write %s fail: %s",
                mfile->file_path.c_str(), baidu::bfs::StrError(ret));
        return false;
    } else if (fsize > g_max_random_write_size) {
        LOG(WARNING, "File %s too large for random write: %ld",
               mfile->file_path.c_str(),  fsize);
        return false;
    }
    mfile->file_size = fsize;
    // Read file to memory, then create a new file for writting.
    mfile->buf_len = std::max(mfile->file_size, 4096L);
    mfile->buf = new char[mfile->buf_len];

    if (mfile->file_size > 0) {
        if (write_only) {
            int ret = bfs_file->Close();
            delete bfs_file;
            bfs_file = NULL;
            if (ret != 0) {
                LOG(WARNING, "Close %s for random write fail: %s",
                        mfile->file_path.c_str(), baidu::bfs::StrError(ret));
                return false;
            }
            ret = g_fs->OpenFile(mfile->file_path.c_str(), O_RDONLY,
                                 &bfs_file, baidu::bfs::ReadOptions());
            if (ret != baidu::bfs::OK) {
                LOG(WARNING, "Open source file %s for random write fail",
                        mfile->file_path.c_str(), baidu::bfs::StrError(ret));
                return false;
            }
        }
        int64_t ret = bfs_file->Read(mfile->buf, mfile->file_size);
        if (ret < mfile->file_size) {
            LOG(WARNING, "Read %s for random write fail: %ld < %ld, %s",
                    mfile->file_path.c_str(), ret, mfile->file_size,
                    baidu::bfs::StrError(ret));
            return false;
        }
    }
    bfs_file->Close();
    delete bfs_file;
    bfs_file = NULL;
    return true;
}

bool is_random_write(MountFile* mfile) {
    if (mfile->buf) {
        assert(mfile->bfs_file == NULL);
        return true;
    } else {
        assert(mfile->bfs_file);
        return false;
    }
}

int bfs_open(const char* path, struct fuse_file_info* finfo) {
    LOG(DEBUG, "Open(%s, %o)", path, finfo->flags);
    baidu::bfs::File* file = NULL;
    int32_t ret = g_fs->OpenFile((g_bfs_path + path).c_str(), O_RDONLY,
                                    &file, baidu::bfs::ReadOptions());
    if (ret != baidu::bfs::OK) {
        LOG(WARNING, "Open %s fail, error code: %s",
                path, baidu::bfs::StrError(ret));
        return EACCES;
    }
    MountFile* mfile = new MountFile(file, path);
    if (finfo->flags & O_RDWR || finfo->flags & O_WRONLY) {
        prepare_for_random_write(mfile, false);
    } else {
        mfile->read_only = true;
    }
    finfo->fh = reinterpret_cast<uint64_t>(mfile);
    return 0;
}

int bfs_read(const char* path, char* buf, size_t len, off_t offset, struct fuse_file_info* finfo) {
    LOG(DEBUG, "Read %s %ld %lu", path, offset, len);
    MountFile* mfile = NULL;
    baidu::bfs::File* file = get_bfs_file(finfo, &mfile);
    int ret = 0;
    if (mfile->buf) {
        // Read from memory.
        if(offset < mfile->file_size) {
            ret = std::min(static_cast<int64_t>(len), mfile->file_size - offset);
            memcpy(buf, mfile->buf + offset, ret);
        }
    } else {
        // Read from dfs.
        ret = file->Pread(buf, len, offset, true);
    }
    LOG(INFO, "Read %s %ld %lu return %d", path, offset, len, ret);
    if (ret < 0) {
        ret = -EIO;
    }
    return ret;
}

int bfs_random_write(MountFile* mfile, const char* buf,
                     size_t len, off_t offset) {
    LOG(INFO, "Random write(%s, %ld, %lu) old offset = %ld",
            mfile->file_path.c_str(), offset, len, mfile->file_size);
    int64_t end_offset = offset + len;
    if (end_offset > g_max_random_write_size) {
        LOG(WARNING, "File %s too large for random write %ld",
                mfile->file_path.c_str(), end_offset);
        return -EIO;
    }
    int new_buf_len = std::min(static_cast<int64_t>(g_max_random_write_size),
                               std::max(mfile->file_size, end_offset * 2));
    if (mfile->buf_len < end_offset) {
        char* new_buf = new char[new_buf_len];
        memcpy(new_buf, mfile->buf, mfile->buf_len);
        delete[] mfile->buf;
        mfile->buf = new_buf;
        mfile->buf_len = new_buf_len;
    }
    memcpy(mfile->buf + offset, buf, len);
    if (mfile->file_size < static_cast<int64_t>(offset + len)) {
        mfile->file_size = offset + len;
    }
    return len;
}

int bfs_write(const char* path, const char* buf, size_t len,
              off_t offset, struct fuse_file_info* finfo) {
    const int zero_buf_size = 256 * 1024;
    static char zero_buf[zero_buf_size] = {0};
    LOG(INFO, "write(%s, %ld, %lu)", path, offset, len);
    MountFile* mfile = NULL;
    baidu::bfs::File* file = get_bfs_file(finfo, &mfile);
    // The first random write ops.
    if (!mfile->buf && mfile->file_size > offset) {
        if(!prepare_for_random_write(mfile, true)) {
            return -EIO;
        }
    }
    // Randon write.
    if (is_random_write(mfile)) {
        return bfs_random_write(mfile, buf, len, offset);
    }

    if (mfile->file_size < offset) {
        // Padding if skip
        LOG(INFO, "write(%s %ld %lu) padding from %ld",
                path, offset, len, mfile->file_size);
        while (mfile->file_size < offset) {
            int blen = std::min(static_cast<int64_t>(zero_buf_size), offset - mfile->file_size);
            int wlen = file->Write(zero_buf, blen);
            if (wlen > 0) {
                mfile->file_size += wlen;
            }
            if (wlen < blen) {
                LOG(WARNING, "write(%s, %ld, %lu) padding at %ld fail",
                        "w %d b: %d", path, offset, len,
                        mfile->file_size, wlen, blen);
                return -EACCES;
            }
        }
    }
    int ret = file->Write(buf, len);
    if (ret > 0) {
        mfile->file_size += ret;
    }
    LOG(INFO, "write(%s, %ld, %lu) return %d", path, offset, len, ret);
    if (ret < 0) {
        ret = -EACCES;
    }
    return ret;
}

int bfs_statfs(const char* path, struct statvfs*) {
    LOG(WARNING, "statfs(%s", path);
    return 0;
}

int bfs_flush(const char* path, struct fuse_file_info* finfo) {
    baidu::bfs::File* file = get_bfs_file(finfo);
    LOG(INFO, "flush(%s)", path);
    int32_t ret = file->Flush();
    if (ret != OK) {
        LOG(WARNING, "fluse %s fail, error code: %s",
                file, baidu::bfs::StrError(ret));
        return EIO;
    }
    return 0;
}

int bfs_fsync(const char* path, int /*datasync*/, struct fuse_file_info* finfo) {
    MountFile* mfile = NULL;
    get_bfs_file(finfo, &mfile);
    baidu::bfs::File*& file = mfile->bfs_file;
    LOG(INFO, "fsync(%s)", path);

    if (mfile->read_only) {
        return 0;
    }
    int retval = 0;
    if (!is_random_write(mfile)) {
        int32_t ret = file->Sync();
        if (ret != baidu::bfs::OK) {
            LOG(WARNING, "fsync(%s) fail, error code: %s",
                    path, baidu::bfs::StrError(ret));
            retval = -EIO;
        }
    } else {
        g_fs->DeleteFile(path);
        int32_t ret = g_fs->OpenFile((g_bfs_path + path).c_str() , O_WRONLY,
                                    0755, &file, baidu::bfs::WriteOptions());
        if (ret != baidu::bfs::OK) {
            LOG(WARNING, "create %s for sync fail, error code: %s",
                    path, baidu::bfs::StrError(ret));
            retval = -EIO;
        }
        int wlen = file->Write(mfile->buf, mfile->file_size);
        if (wlen < mfile->file_size) {
            LOG(WARNING, "write(%s, %ld) for fsync fail", path, mfile->file_size);
            delete file;
            file = NULL;
            retval = -EIO;
        } else {
            LOG(INFO, "write(%s, %ld) for fsync", path, mfile->file_size);
            delete[] mfile->buf;
            mfile->buf = NULL;
        }
    }
    return retval;
}

int bfs_release(const char* path, struct fuse_file_info* finfo) {
    LOG(INFO, "release(%s", path);
    int retval = bfs_fsync(path, 0, finfo);

    MountFile* mfile = NULL;
    baidu::bfs::File* file = get_bfs_file(finfo, &mfile);
    int ret = file->Close();
    if (ret != 0) {
        LOG(WARNING, "Close file %s fail", mfile->file_path.c_str());
        retval = -EIO;
    }
    delete file;
    delete mfile;
    return retval;
}

/** Set extended attributes */
int (*setxattr) (const char *, const char *, const char *, size_t, int);

/** Get extended attributes */
int (*getxattr) (const char *, const char *, char *, size_t);
int bfs_getxattr(const char * path, const char* key, char *, size_t) {
    LOG(WARNING, "bfs_getxattr(%s, %s", path, key);
    return 0;
}

/** List extended attributes */
int (*listxattr) (const char *, char *, size_t);

/** Remove extended attributes */
int (*removexattr) (const char *, const char *);

int bfs_opendir(const char* path, struct fuse_file_info *) {
    fprintf(stderr, "opendir(%s)\n", path);
    LOG(WARNING, "opendir(%s)", path);
    return 0;
}

int bfs_readdir(const char* path, void* buf, fuse_fill_dir_t filler,
                              off_t offset, struct fuse_file_info* fi) {
    //return filler(buf, "hello-world", NULL, 0);
    LOG(DEBUG, "readdir(%s", path);
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
    LOG(WARNING, "readdir(%s)", path);
    return 0;
}

int bfs_fsyncdir(const char* path, int, struct fuse_file_info*) {
    LOG(WARNING, "fsyncdir(%s)", path);
    return 0;
}

int bfs_access(const char* path, int mode) {
    LOG(WARNING, "access(%s, %d)", path, mode);
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
    LOG(INFO, "create(%s, %o, %o)", path, mode, finfo->flags);
    baidu::bfs::File* file = NULL;
    int32_t ret = g_fs->OpenFile((g_bfs_path + path).c_str() , O_WRONLY, mode,
                                 &file, baidu::bfs::WriteOptions());
    if (ret != baidu::bfs::OK) {
        LOG(WARNING, "create %s, fail, error code: %s",
                path, baidu::bfs::StrError(ret));
        return EACCES;
    }
    MountFile* mfile = new MountFile(file, path);
    finfo->fh = reinterpret_cast<uint64_t>(mfile);
    return 0;
}

int bfs_ftruncate(const char* path, off_t offset, struct fuse_file_info* finfo) {
    LOG(INFO, "ftruncate(%s, %ld", path, offset);
    MountFile* mfile = NULL;
    get_bfs_file(finfo, &mfile);
    mfile->file_size = offset;
    return 0;
}

int bfs_fgetattr(const char* path, struct stat* st, struct fuse_file_info*) {
    LOG(INFO, "fgetattr(%s", path);
    return bfs_getattr(path, st);
}

int bfs_lock(const char* path, struct fuse_file_info *, int cmd, struct flock *) {
    LOG(WARNING, "lock(%s, %d)", path, cmd);
    return 0;
}

int bfs_utimens(const char* path, const struct timespec tv[2]) {
    LOG(WARNING, "utimes(%s)", path);
    return 0;
}

int bfs_bmap(const char* path, size_t blocksize, uint64_t *idx) {
    LOG(WARNING, "bmap(%s)", path);
    return 0;
}

void* bfs_init(struct fuse_conn_info *conn) {
    LOG(INFO, "init");
    if (g_bfs_cluster.empty()) {
        //g_bfs_cluster = "yq01-tera60.yq01:8828";
        g_bfs_cluster = "localhost:8828";
    }
    if (!baidu::bfs::FS::OpenFileSystem(g_bfs_cluster.c_str(), &g_fs, baidu::bfs::FSOptions())) {
        LOG(ERROR, "Open file system %s fail", g_bfs_cluster.c_str());
        abort();
    }
    int32_t ret = g_fs->Access(g_bfs_path.c_str(), R_OK | W_OK);
    if (ret != baidu::bfs::OK) {
        LOG(ERROR, "access %s fail, error code: %s",
                g_bfs_path.c_str(), baidu::bfs::StrError(ret));
        abort();
    }
    return g_fs;
}

void bfs_destroy(void*) {
    LOG(WARNING, "destroy");
}

int parse_args(int* argc, char* argv[]) {
    if (*argc < 2) {
        fprintf(stderr, "Usage: %s mount_point [-d]"
                        " [-c bfs_cluster_addr]"
                        " [-p bfs_path]\n",
                argv[0]);
        fprintf(stderr, "\t-d                    Fuse debug (optional)\n"
                        "\t-c bfs_cluster_addr   Ip:port\n"
                        "\t-p bfs_path           The path in  which you mount to the mount_point\n"
                        "Example:\n"
                        "       %s /mnt/bfs -d -c 127.0.0.1:8827 -p /\n",
                argv[0]);
        return 1;
    }

    for (int i = 1; i + 1 < *argc; i++) {
        if (strncmp(argv[i], "-c", 2) == 0) {
            g_bfs_cluster = argv[i + 1];
            LOG(INFO, "Use cluster: %s", g_bfs_cluster.c_str());
        } else if (strncmp(argv[i], "-p", 2) == 0) {
            g_bfs_path = argv[i + 1];
            LOG(INFO, "Use path %s", g_bfs_path.c_str());
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

} // namespace bfs
} // namespace baidu

int main(int argc, char* argv[]) {
    if (baidu::bfs::parse_args(&argc, argv)) {
        return 1;
    }

    static struct fuse_operations ops;
    ops.getattr = baidu::bfs::bfs_getattr,
    ops.readlink = baidu::bfs::bfs_readlink,
    ops.mknod = baidu::bfs::bfs_mknod;
    ops.mkdir = baidu::bfs::bfs_mkdir;
    ops.unlink = baidu::bfs::bfs_ulink;
    ops.rmdir = baidu::bfs::bfs_rmdir;
    ops.symlink = baidu::bfs::bfs_symlink;
    ops.rename = baidu::bfs::bfs_rename;
    ops.link = baidu::bfs::bfs_link;
    ops.chmod = baidu::bfs::bfs_chmod;
    ops.chown = baidu::bfs::bfs_chown;
    ops.truncate = baidu::bfs::bfs_truncate;
    ops.open = baidu::bfs::bfs_open;
    ops.read = baidu::bfs::bfs_read;
    ops.write = baidu::bfs::bfs_write;
    ops.statfs = baidu::bfs::bfs_statfs;
    ops.flush = baidu::bfs::bfs_flush;
    ops.release = baidu::bfs::bfs_release;
    ops.fsync = baidu::bfs::bfs_fsync;
    ops.opendir = baidu::bfs::bfs_opendir;
    ops.readdir = baidu::bfs::bfs_readdir;
    ops.getxattr = baidu::bfs::bfs_getxattr;
    ops.releasedir = baidu::bfs::bfs_releasedir;
    ops.fsyncdir = baidu::bfs::bfs_fsyncdir;
    ops.init = baidu::bfs::bfs_init;
    ops.destroy = baidu::bfs::bfs_destroy;
    ops.access = baidu::bfs::bfs_access;
    ops.create = baidu::bfs::bfs_create;
    ops.ftruncate = baidu::bfs::bfs_ftruncate;
    ops.fgetattr = baidu::bfs::bfs_fgetattr;
    ops.lock = baidu::bfs::bfs_lock;
    ops.utimens = baidu::bfs::bfs_utimens;
    ops.bmap = baidu::bfs::bfs_bmap;

    return fuse_main(argc, argv, &ops, NULL);
}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
