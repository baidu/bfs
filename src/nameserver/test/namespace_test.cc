// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#define private public
#include "nameserver/namespace.h"
#include "proto/status_code.pb.h"

#include <common/util.h>
#include <common/string_util.h>
#include <common/thread_pool.h>
#include <fcntl.h>
#include <functional>

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

bool CreateTree(NameSpace* ns) {
    std::vector<int64_t> blocks_to_remove;
    int ret = ns->CreateFile("/file1", 0, 0, -1, &blocks_to_remove);
    ret |= ns->CreateFile("/file2", 0, 0, -1, &blocks_to_remove);
    ret |= ns->CreateFile("/dir1/subdir1/file3", 0, 0, -1, &blocks_to_remove);
    ret |= ns->CreateFile("/dir1/subdir1/file4", 0, 0, -1, &blocks_to_remove);
    ret |= ns->CreateFile("/dir1/subdir2/file5", 0, 0, -1, &blocks_to_remove);
    ret |= ns->CreateFile("/xdir", 0, 01755, -1, &blocks_to_remove);
    return ret == kOK;
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
    ASSERT_EQ(kOK, ns.CreateFile("/file1", 0, 0, -1, &blocks_to_remove));
    ASSERT_NE(kOK, ns.CreateFile("/file1", 0, 0, -1, &blocks_to_remove));
    ASSERT_EQ(kOK, ns.CreateFile("/file2", 0, 0, 0, &blocks_to_remove));
    ASSERT_EQ(kOK, ns.CreateFile("/file3", 0, 0, 2, &blocks_to_remove));
    ASSERT_EQ(kOK, ns.CreateFile("/dir1/subdir1/file1", 0, 0, -1, &blocks_to_remove));
    ASSERT_EQ(kOK, ns.CreateFile("/dir1/subdir1/file1", O_TRUNC, 0, -1, &blocks_to_remove));
    ASSERT_EQ(kOK, ns.CreateFile("/dir1/subdir2/file1", 0, 0, -1, &blocks_to_remove));
    ASSERT_EQ(kOK, ns.CreateFile("/dir1/subdir2/file2", 0, -1, -1, &blocks_to_remove));
    ASSERT_EQ(kOK, ns.CreateFile("/dir1/subdir2/file3", 0, 01755, -1, &blocks_to_remove));
}

TEST_F(NameSpaceTest, List) {
    FLAGS_namedb_path = "./db";
    system("rm -rf ./db");
    NameSpace ns;
    ASSERT_TRUE(CreateTree(&ns));
    google::protobuf::RepeatedPtrField<FileInfo> outputs;
    ASSERT_EQ(kOK, ns.ListDirectory("/dir1", &outputs));
    ASSERT_EQ(2, outputs.size());
    ASSERT_EQ(std::string("subdir1"), outputs.Get(0).name());
    ASSERT_EQ(std::string("subdir2"), outputs.Get(1).name());
}

TEST_F(NameSpaceTest, Rename) {
    NameSpace ns;
    bool need_unlink;
    FileInfo remove_file;
    /// self -> self
    ASSERT_EQ(kBadParameter, ns.Rename("/dir1", "/dir1", &need_unlink, &remove_file));
    /// dir -> none
    ASSERT_EQ(kOK, ns.Rename("/dir1/subdir1", "/dir1/subdir3", &need_unlink, &remove_file));
    ASSERT_FALSE(need_unlink);
    /// dir -> existing dir
    ASSERT_NE(kOK, ns.Rename("/dir1/subdir2", "/dir1/subdir3", &need_unlink, &remove_file));
    ASSERT_FALSE(need_unlink);
    /// parent dir -> subdir
    ASSERT_EQ(kBadParameter, ns.Rename("/dir1/", "/dir1/subdir4", &need_unlink, &remove_file));
    /// file -> not exist parent
    ASSERT_NE(kOK, ns.Rename("/file1", "/dir1/subdir4/file1", &need_unlink, &remove_file));
    /// file -> existing dir
    ASSERT_NE(kOK, ns.Rename("/file1", "/dir1/subdir3", &need_unlink, &remove_file));
    ASSERT_FALSE(need_unlink);
    /// file -> none
    ASSERT_EQ(kOK, ns.Rename("/file1", "/dir1/subdir3/file1", &need_unlink, &remove_file));
    ASSERT_FALSE(need_unlink);
    /// noe -> none
    ASSERT_NE(kOK, ns.Rename("/file1", "/dir1/subdir3/file2", &need_unlink, &remove_file));
    ASSERT_FALSE(need_unlink);
    /// file -> existing file
    ASSERT_EQ(kOK, ns.Rename("/dir1/subdir2/file5", "/dir1/subdir3/file1", &need_unlink, &remove_file));
    ASSERT_TRUE(need_unlink);
    ASSERT_EQ(remove_file.entry_id(), 2);

    /// Root dir to root dir
    ASSERT_NE(kOK, ns.Rename("/", "/dir2", &need_unlink, &remove_file));
    ASSERT_EQ(kOK, ns.Rename("/dir1", "/dir2", &need_unlink, &remove_file));

    /// Deep rename
    std::vector<int64_t> blocks_to_remove;
    ASSERT_EQ(kOK, ns.CreateFile("/tera/meta/0/00000001.dbtmp", 0, 0, -1, &blocks_to_remove));
    ASSERT_EQ(kOK, ns.Rename("/tera/meta/0/00000001.dbtmp", "/tera/meta/0/CURRENT", &need_unlink, &remove_file));
    ASSERT_FALSE(need_unlink);
    ASSERT_TRUE(ns.LookUp("/tera/meta/0/CURRENT", &remove_file));
}

TEST_F(NameSpaceTest, RemoveFile) {
    FLAGS_namedb_path = "./db";
    system("rm -rf ./db");
    NameSpace ns;
    ASSERT_TRUE(CreateTree(&ns));
    FileInfo file_removed;
    ASSERT_EQ(kBadParameter, ns.RemoveFile("/", &file_removed));
    ASSERT_EQ(kBadParameter, ns.RemoveFile("/dir1", &file_removed));
    ASSERT_EQ(kOK, ns.RemoveFile("/file2",&file_removed));
    ASSERT_EQ(3, file_removed.entry_id());
    ASSERT_EQ(kBadParameter, ns.RemoveFile("/", &file_removed));
    ASSERT_EQ(kOK, ns.RemoveFile("/file1", &file_removed));
    ASSERT_EQ(2, file_removed.entry_id());
    ASSERT_EQ(kNsNotFound, ns.RemoveFile("/file2", &file_removed));
    ASSERT_EQ(kNsNotFound, ns.RemoveFile("/file3", &file_removed));
}

TEST_F(NameSpaceTest, DeleteDirectory) {
    FLAGS_namedb_path = "./db";
    system("rm -rf ./db");
    NameSpace ns;
    ASSERT_TRUE(CreateTree(&ns));
    std::vector<FileInfo> files_removed;

    // Delete not empty
    ASSERT_EQ(kDirNotEmpty, ns.DeleteDirectory("/dir1", false, &files_removed));
    ASSERT_EQ(kDirNotEmpty, ns.DeleteDirectory("/dir1/subdir2", false, &files_removed));
    // Delete empty
    ASSERT_EQ(kOK, ns.DeleteDirectory("/xdir", true, &files_removed));
    // Delete root dir
    ASSERT_EQ(kDirNotEmpty, ns.DeleteDirectory("/", false, &files_removed));
    // Delete subdir
    ASSERT_EQ(0, ns.DeleteDirectory("/dir1/subdir2", true, &files_removed));
    ASSERT_EQ(files_removed.size(), 1U);
    ASSERT_EQ(files_removed[0].entry_id(), 9);
    // List after delete
    google::protobuf::RepeatedPtrField<FileInfo> outputs;
    ASSERT_EQ(kOK, ns.ListDirectory("/dir1", &outputs));
    ASSERT_EQ(1, outputs.size());
    ASSERT_EQ(std::string("subdir1"), outputs.Get(0).name());

    // Delete another subdir
    printf("Delete another subdir\n");
    ASSERT_EQ(kDirNotEmpty, ns.DeleteDirectory("/dir1/subdir1", false, &files_removed));
    ASSERT_EQ(kOK, ns.ListDirectory("/dir1/subdir1", &outputs));
    ASSERT_EQ(2, outputs.size());

    ASSERT_EQ(kOK, ns.DeleteDirectory("/dir1", true, &files_removed));
    ASSERT_EQ(files_removed.size(), 2U);
    ASSERT_EQ(files_removed[0].entry_id(), 6);
    ASSERT_EQ(files_removed[1].entry_id(), 7);
    ASSERT_EQ(kNsNotFound, ns.ListDirectory("/dir1/subdir1", &outputs));
    ASSERT_EQ(kNsNotFound, ns.ListDirectory("/dir1", &outputs));

    // Use rmr to delete a file
    ASSERT_EQ(kBadParameter, ns.DeleteDirectory("/file1", true, &files_removed));
    // Rmr root path will clear the filesystem
    ASSERT_EQ(kOK, ns.DeleteDirectory("/", true, &files_removed));
    ASSERT_EQ(kOK, ns.ListDirectory("/", &outputs));
    ASSERT_EQ(0, outputs.size());
}

TEST_F(NameSpaceTest, DeleteDirectory2) {
    FLAGS_namedb_path = "./db";
    system("rm -rf ./db");
    NameSpace ns;
    std::vector<int64_t> blocks_to_remove;
    ns.CreateFile("/tera", 0, 01755, -1, &blocks_to_remove);
    ns.CreateFile("/file1", 0, 0, -1, &blocks_to_remove);
    ns.CreateFile("/tera/file2", 0, 0, -1, &blocks_to_remove);
    std::vector<FileInfo> files_removed;
    ns.DeleteDirectory("/", true, &files_removed);
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

void UpdateFileInfoHelper(NameSpace* ns, int n) {
    NameServerLog log;
    for (auto i = 0; i <= n; i++) {
        FileInfo info;
        info.add_blocks(ns->GetNewBlockId());
        ns->UpdateFileInfo(info, &log);
    }
}

TEST_F(NameSpaceTest, GetNewBlockId) {
    system("rm -rf ./db");
    FLAGS_block_id_allocation_size = 100;
    ThreadPool tp(3);
    {
        NameSpace ns;
        for (auto i = 0; i < 10; i++) {
            tp.AddTask(std::bind(&UpdateFileInfoHelper, &ns, 110));
        }
        tp.Stop(true);
    }
    {
        NameSpace ns;
        ASSERT_EQ(ns.GetNewBlockId(), 1201);
    }
    system("rm -rf ./db");
}

}
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
