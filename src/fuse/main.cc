#define FUSE_USE_VERSION 26
#include <fuse.h>
#include <errno.h>
#include "sdk/bfs.h"

baidu::bfs::FS* fs;

static int dfs_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
                       off_t offset, struct fuse_file_info *fi)
{
    return 0;
}

static int dfs_mkdir(const char* dir, mode_t mode)
{
    (void) mode;
    bool ret = fs->CreateDirectory(dir);
    if (!ret) 
    {
        fprintf(stderr, "Create dir %s fail\n", dir);
        return -EIO;
    }
    return 0;
}

static int dfs_open(const char *path, struct fuse_file_info *fi)
{
    if (path == NULL)
    {
        return -EINVAL;
    }

    baidu::bfs::File* file;
    if (!fs->OpenFile(path, fi->flags, &file)) 
    {
        return -EINVAL;
    }
    fi->fh = (uint64_t)file;
    return 0;
}

static int dfs_write(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *info)
{
    // baidu::bfs::File* file = (baidu::bfs::File* )info->fh;
    return 0;
}

int dfs_getattr(const char *path, struct stat *st)
{
	  if (path == NULL) 
    {
		    return -EINVAL;
	  }	

    baidu::bfs::BfsFileInfo fileinfo;
    if (!fs->Stat(path, &fileinfo))
    {
        fprintf(stderr, "bfs stat error, maybe not exists\n");
		    return -ENOENT;
    }

    memset(st, 0, sizeof(struct stat));
    st->st_mode = fileinfo.mode;
    st->st_size = fileinfo.size;
    st->st_nlink = 2;
    st->st_atime = fileinfo.ctime;
    st->st_ctime = fileinfo.ctime;
    st->st_mtime = fileinfo.ctime;
    st->st_mtime = fileinfo.ctime;
    st->st_blksize = 512;
    st->st_blocks = 1;
    // st->st_blksize  	= (blksize_t)fileinfo.blocks.size();
    // st->st_uid      	= default_id;
	  // st->st_gid      	= default_id;
    return 0;
}

int main(int argc, char *argv[])
{
    if (argc < 2)
    {
        fprintf(stderr, "usage %s [-d] mount_point\n", argv[0]);
        return 0;
    }

    // std::string ns_address(argv[argc - 1]);
    if (!baidu::bfs::FS::OpenFileSystem("127.0.0.1:8828", &fs)) 
    {
        fprintf(stderr, "Open filesytem 127.0.0.1:8828 fail\n");
        return 1;
    }

    static struct fuse_operations oper; 
    oper.getattr = dfs_getattr;
    oper.readdir = dfs_readdir;
    oper.open = dfs_open;
    oper.mkdir = dfs_mkdir;
    oper.write = dfs_write;
    oper.read = NULL;
    return fuse_main(argc, argv, &oper, NULL);
}
