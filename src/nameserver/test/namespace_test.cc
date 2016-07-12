// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#define private public
#include "nameserver/namespace.h"

#include <common/util.h>
#include <common/string_util.h>
#include <fcntl.h>

#include <gflags/gflags.h>
#include <gtest/gtest.h>

DECLARE_string(namedb_path);
DECLARE_int32(block_id_allocation_size);

namespace baidu {
namespace bfs {

class NameSpaceTest : public ::testing::Test {
public:
    NameSpaceTest() {}
protected:
};

TEST_F(NameSpaceTest, EncodingStoreKey) {
    std::string key_start;
    NameSpace::EncodingStoreKey(5, "", &key_start);
    ASSERT_EQ(key_start.size(), 8U);
    for (int i = 0; i < 7; i++) {
        ASSERT_EQ(key_start[i], 0);
    }
    ASSERT_EQ(key_start[7], '\5');

    NameSpace::EncodingStoreKey(3<<16, "/home", &key_start);
    ASSERT_EQ(key_start.size(), 13U);
    ASSERT_EQ(key_start[5], 3);
    ASSERT_EQ(key_start.substr(8), std::string("/home"));
}

TEST_F(NameSpaceTest, SplitPath) {
    std::vector<std::string> element;
    ASSERT_TRUE(common::util::SplitPath("/home", &element));
    ASSERT_EQ(1U, element.size());
    ASSERT_EQ(element[0], std::string("home"));
    ASSERT_TRUE(common::util::SplitPath("/dir1/subdir1/file3", &element));
    ASSERT_EQ(3U, element.size());
    ASSERT_EQ(element[0], std::string("dir1"));
    ASSERT_EQ(element[1], std::string("subdir1"));
    ASSERT_EQ(element[2], std::string("file3"));
    ASSERT_TRUE(common::util::SplitPath("/", &element));
    ASSERT_EQ(0U, element.size());
    ASSERT_FALSE(common::util::SplitPath("", &element));

}

int CreateFile(const std::string& file_name, int flags, int mode,
                int replica_num, std::vector<int64_t>* blocks_to_remove, NameSpace* ns) {
    NameServerLog log;
    int ret = ns->CreateFile(file_name, flags, mode, replica_num, blocks_to_remove, &log);
    std::string logstr;
    log.SerializeToString(&logstr);
    ns->TailLog(logstr, -1);
    return ret;
}

int Rename(const std::string& old_path, const std::string& new_path, bool* need_unlink,
            FileInfo* remove_file, NameSpace* ns) {
    NameServerLog log;
    int ret = ns->Rename(old_path, new_path, need_unlink, remove_file, &log);
    std::string logstr;
    log.SerializeToString(&logstr);
    ns->TailLog(logstr, -1);
    return ret;
}

int RemoveFile(const std::string& path, FileInfo* file_removed, NameSpace* ns) {
    NameServerLog log;
    int ret = ns->RemoveFile(path, file_removed, &log);
    std::string logstr;
    log.SerializeToString(&logstr);
    ns->TailLog(logstr, -1);
    return ret;
}

int DeleteDirectory(const std::string& path, bool recursive,
                     std::vector<FileInfo>* files_removed, NameSpace* ns) {
    NameServerLog log;
    int ret = ns->DeleteDirectory(path, recursive, files_removed, &log);
    std::string logstr;
    log.SerializeToString(&logstr);
    ns->TailLog(logstr, -1);
    return ret;
}

int CreateTree(NameSpace* ns) {
    std::vector<int64_t> blocks_to_remove;
    int ret = CreateFile("/file1", 0, 0, -1, &blocks_to_remove, ns);
    ret |= CreateFile("/file2", 0, 0, -1, &blocks_to_remove, ns);
    ret |= CreateFile("/dir1/subdir1/file3", 0, 0, -1, &blocks_to_remove, ns);
    ret |= CreateFile("/dir1/subdir1/file4", 0, 0, -1, &blocks_to_remove, ns);
    ret |= CreateFile("/dir1/subdir2/file5", 0, 0, -1, &blocks_to_remove, ns);
    ret |= CreateFile("/xdir", 0, 01755, -1, &blocks_to_remove, ns);
    return ret == 0;
}

TEST_F(NameSpaceTest, NameSpace) {
    std::string version;
    {
        FLAGS_namedb_path = "./db";
        system("rm -rf ./db");
        NameSpace init_ns;
        leveldb::Iterator* it = init_ns.db_->NewIterator(leveldb::ReadOptions());
        it->Seek(std::string(8, 0) + "version");
        ASSERT_TRUE(it->Valid());
        ASSERT_EQ(it->key().ToString(), std::string(8, 0) + "version");
        version = it->value().ToString();
        delete it;
    }

    // Load  namespace with old version
    NameSpace ns;
    CreateTree(&ns);
    leveldb::Iterator* it = ns.db_->NewIterator(leveldb::ReadOptions());
    it->Seek(std::string(8, 0) + "version");
    ASSERT_EQ(version, it->value().ToString());

    // Iterate namespace
    it->Seek(std::string(7, 0) + '\1');
    ASSERT_TRUE(it->Valid());
    std::string entry1(it->key().data(), it->key().size());
    ASSERT_EQ(entry1.substr(8), std::string("dir1"));

    // next entry
    it->Next();
    // key
    std::string entry2(it->key().data(), it->key().size());
    ASSERT_EQ(entry2.substr(8), std::string("file1"));
    ASSERT_EQ(entry2[0], 0);
    ASSERT_EQ(*reinterpret_cast<int*>(&entry2[1]), 0);
    ASSERT_EQ(entry2[7], 1);
    // value
    FileInfo info;
    ASSERT_TRUE(info.ParseFromArray(it->value().data(), it->value().size()));
    ASSERT_EQ(2, info.entry_id());

    delete it;
}

TEST_F(NameSpaceTest, CreateFile) {
    FLAGS_namedb_path = "./db";
    system("rm -rf ./db");
    NameSpace ns;
    std::vector<int64_t> blocks_to_remove;
    ASSERT_EQ(0, CreateFile("/file1", 0, 0, -1, &blocks_to_remove, &ns));
    ASSERT_NE(0, CreateFile("/file1", 0, 0, -1, &blocks_to_remove, &ns));
    ASSERT_EQ(0, CreateFile("/file2", 0, 0, 0, &blocks_to_remove, &ns));
    ASSERT_EQ(0, CreateFile("/file3", 0, 0, 2, &blocks_to_remove, &ns));
    ASSERT_EQ(0, CreateFile("/dir1/subdir1/file1", 0, 0, -1, &blocks_to_remove, &ns));
    ASSERT_EQ(0, CreateFile("/dir1/subdir1/file1", O_TRUNC, 0, -1, &blocks_to_remove, &ns));
    ASSERT_EQ(0, CreateFile("/dir1/subdir2/file1", 0, 0, -1, &blocks_to_remove, &ns));
    ASSERT_EQ(0, CreateFile("/dir1/subdir2/file2", 0, -1, -1, &blocks_to_remove, &ns));
    ASSERT_EQ(0, CreateFile("/dir1/subdir2/file3", 0, 01755, -1, &blocks_to_remove, &ns));
}

TEST_F(NameSpaceTest, List) {
    FLAGS_namedb_path = "./db";
    system("rm -rf ./db");
    NameSpace ns;
    ASSERT_TRUE(CreateTree(&ns));
    google::protobuf::RepeatedPtrField<FileInfo> outputs;
    ASSERT_EQ(0, ns.ListDirectory("/dir1", &outputs));
    ASSERT_EQ(2, outputs.size());
    ASSERT_EQ(std::string("subdir1"), outputs.Get(0).name());
    ASSERT_EQ(std::string("subdir2"), outputs.Get(1).name());
}

TEST_F(NameSpaceTest, Rename) {
    NameSpace ns;
    bool need_unlink;
    FileInfo remove_file;
    /// dir -> none
    ASSERT_EQ(0, Rename("/dir1/subdir1", "/dir1/subdir3", &need_unlink, &remove_file, &ns));
    ASSERT_FALSE(need_unlink);
    /// dir -> existing dir
    ASSERT_NE(0, Rename("/dir1/subdir2", "/dir1/subdir3", &need_unlink, &remove_file, &ns));
    ASSERT_FALSE(need_unlink);
    /// file -> not exist parent
    ASSERT_NE(0, Rename("/file1", "/dir1/subdir4/file1", &need_unlink, &remove_file, &ns));
    /// file -> existing dir
    ASSERT_NE(0, Rename("/file1", "/dir1/subdir3", &need_unlink, &remove_file, &ns));
    ASSERT_FALSE(need_unlink);
    /// file -> none
    ASSERT_EQ(0, Rename("/file1", "/dir1/subdir3/file1", &need_unlink, &remove_file, &ns));
    ASSERT_FALSE(need_unlink);
    /// noe -> none
    ASSERT_NE(0, Rename("/file1", "/dir1/subdir3/file2", &need_unlink, &remove_file, &ns));
    ASSERT_FALSE(need_unlink);
    /// file -> existing file
    ASSERT_EQ(0, Rename("/dir1/subdir2/file5", "/dir1/subdir3/file1", &need_unlink, &remove_file, &ns));
    ASSERT_TRUE(need_unlink);
    ASSERT_EQ(remove_file.entry_id(), 2);

    /// Root dir to root dir
    ASSERT_NE(0, Rename("/", "/dir2", &need_unlink, &remove_file, &ns));
    ASSERT_EQ(0, Rename("/dir1", "/dir2", &need_unlink, &remove_file, &ns));

    /// Deep rename
    std::vector<int64_t> blocks_to_remove;
    ASSERT_EQ(0, CreateFile("/tera/meta/0/00000001.dbtmp", 0, 0, -1, &blocks_to_remove, &ns));
    ASSERT_EQ(0, Rename("/tera/meta/0/00000001.dbtmp", "/tera/meta/0/CURRENT", &need_unlink, &remove_file, &ns));
    ASSERT_FALSE(need_unlink);
    ASSERT_TRUE(ns.LookUp("/tera/meta/0/CURRENT", &remove_file));
}

TEST_F(NameSpaceTest, RemoveFile) {
    FLAGS_namedb_path = "./db";
    system("rm -rf ./db");
    NameSpace ns;
    ASSERT_TRUE(CreateTree(&ns));
    FileInfo file_removed;
    ASSERT_NE(0, RemoveFile("/", &file_removed, &ns));
    ASSERT_NE(0, RemoveFile("/dir1", &file_removed, &ns));
    ASSERT_EQ(0, RemoveFile("/file2",&file_removed, &ns));
    ASSERT_EQ(3, file_removed.entry_id());
    ASSERT_NE(0, RemoveFile("/",&file_removed, &ns));
    ASSERT_EQ(0, RemoveFile("/file1",&file_removed, &ns));
    ASSERT_EQ(2, file_removed.entry_id());
    ASSERT_NE(0, RemoveFile("/file2",&file_removed, &ns));
    ASSERT_NE(0, RemoveFile("/file3",&file_removed, &ns));
}

TEST_F(NameSpaceTest, DeleteDirectory) {
    FLAGS_namedb_path = "./db";
    system("rm -rf ./db");
    NameSpace ns;
    ASSERT_TRUE(CreateTree(&ns));
    std::vector<FileInfo> files_removed;

    // Delete not empty
    ASSERT_NE(0, DeleteDirectory("/dir1", false, &files_removed, &ns));
    ASSERT_NE(0, DeleteDirectory("/dir1/subdir2", false, &files_removed, &ns));
    // Delete empty
    ASSERT_EQ(0, DeleteDirectory("/xdir", true, &files_removed, &ns));
    // Delete root dir
    ASSERT_NE(0, DeleteDirectory("/", false, &files_removed, &ns));
    // Delete subdir
    ASSERT_EQ(0, DeleteDirectory("/dir1/subdir2", true, &files_removed, &ns));
    ASSERT_EQ(files_removed.size(), 1U);
    ASSERT_EQ(files_removed[0].entry_id(), 9);
    // List after delete
    google::protobuf::RepeatedPtrField<FileInfo> outputs;
    ASSERT_EQ(0, ns.ListDirectory("/dir1", &outputs));
    ASSERT_EQ(1, outputs.size());
    ASSERT_EQ(std::string("subdir1"), outputs.Get(0).name());

    // Delete another subdir
    printf("Delete another subdir\n");
    ASSERT_NE(0, DeleteDirectory("/dir1/subdir1", false, &files_removed, &ns));
    ASSERT_EQ(0, ns.ListDirectory("/dir1/subdir1", &outputs));
    ASSERT_EQ(2, outputs.size());

    ASSERT_EQ(0, DeleteDirectory("/dir1", true, &files_removed, &ns));
    ASSERT_EQ(files_removed.size(), 2U);
    ASSERT_EQ(files_removed[0].entry_id(), 6);
    ASSERT_EQ(files_removed[1].entry_id(), 7);
    ASSERT_NE(0, ns.ListDirectory("/dir1/subdir1", &outputs));
    ASSERT_NE(0, ns.ListDirectory("/dir1", &outputs));

    // Use rmr to delete a file
    ASSERT_EQ(1, DeleteDirectory("/file1", true, &files_removed, &ns));
    // Rmr root path will clear the filesystem
    ASSERT_EQ(0, DeleteDirectory("/", true, &files_removed, &ns));
    ASSERT_EQ(0, ns.ListDirectory("/", &outputs));
    ASSERT_EQ(0, outputs.size());
}

TEST_F(NameSpaceTest, DeleteDirectory2) {
    FLAGS_namedb_path = "./db";
    system("rm -rf ./db");
    NameSpace ns;
    std::vector<int64_t> blocks_to_remove;
    CreateFile("/tera", 0, 01755, -1, &blocks_to_remove, &ns);
    CreateFile("/file1", 0, 0, -1, &blocks_to_remove, &ns);
    CreateFile("/tera/file2", 0, 0, -1, &blocks_to_remove, &ns);
    std::vector<FileInfo> files_removed;
    DeleteDirectory("/", true, &files_removed, &ns);
    ASSERT_EQ(files_removed.size(), 2UL);
    google::protobuf::RepeatedPtrField<FileInfo> outputs;
    ns.ListDirectory("/", &outputs);
    ASSERT_EQ(outputs.size(), 0);
}

TEST_F(NameSpaceTest, GetFileInfo) {
    NameSpace ns;
    FileInfo info;
    ASSERT_TRUE(ns.GetFileInfo("/", &info));
}

TEST_F(NameSpaceTest, NormalizePath) {
    ASSERT_EQ(NameSpace::NormalizePath("home") , std::string("/home"));
    ASSERT_EQ(NameSpace::NormalizePath("") , std::string("/"));
    ASSERT_EQ(NameSpace::NormalizePath("/") , std::string("/"));
    ASSERT_EQ(NameSpace::NormalizePath("///") , std::string("/"));
    ASSERT_EQ(NameSpace::NormalizePath("//home") , std::string("/home"));
    ASSERT_EQ(NameSpace::NormalizePath("/home//") , std::string("/home"));
    ASSERT_EQ(NameSpace::NormalizePath("//home/") , std::string("/home"));
    ASSERT_EQ(NameSpace::NormalizePath("//home/work/") , std::string("/home/work"));
}

TEST_F(NameSpaceTest, GetNewBlockId) {
    system("rm -rf ./db");
    FLAGS_block_id_allocation_size = 10000;
    {
        NameSpace ns;
        for (int i = 1; i <= 20010; i++) {
            ASSERT_EQ(ns.GetNewBlockId(NULL), i);
        }
    }
    {
        NameSpace ns;
        for (int i = 1; i <= 20010; i++) {
            ASSERT_EQ(ns.GetNewBlockId(NULL), i + 30000);
        }
    }

}

}
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
