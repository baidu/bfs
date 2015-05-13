// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#include "dfs.h"
#include <dlfcn.h>
#include <string>

const char* so_path = "./bfs_wrapper.so";
const char* dfs_conf = "yq01-tera60.yq01:8828";

const std::string test_path("/test");

#define ASSERT_TRUE(x) \
    do { \
        bool ret = x; \
        if (!ret) { \
            printf("\033[31m[Test failed]\033[0m: %d, %s\n", __LINE__, #x); \
            exit(1); \
        } else { \
            printf("\033[32m[Test pass]\033[0m: %s\n", #x); \
        } \
    } while(0)

int main() {
    dlerror();
    void* handle = dlopen(so_path, RTLD_NOW);
    const char* err = dlerror();
    if (handle == NULL) {
        fprintf(stderr, "Open %s fail: %s\n", so_path, err);
        return 1;
    }

    leveldb::DfsCreator creator = (leveldb::DfsCreator)dlsym(handle, "NewDfs");
    err = dlerror();
    if (err != NULL) {
        fprintf(stderr, "Load NewDfs from %s fail: %s\n", so_path, err);
        return 1;
    }

    leveldb::Dfs* dfs = (*creator)(dfs_conf);
    
    ASSERT_TRUE(dfs != NULL);
    ASSERT_TRUE(0 == dfs->CreateDirectory(test_path));

    /// Open
    std::string file1 = test_path + "/file1";
    std::string file2 = test_path + "/file2";

    if (0 == dfs->Exists(file1)) {
        ASSERT_TRUE(0 == dfs->Delete(file1));
    }

    leveldb::DfsFile* fp1 = dfs->OpenFile(file1, leveldb::WRONLY);
    ASSERT_TRUE(fp1 != NULL);
    
    /// Write&Sync
    char content1[] = "File1 content";
    char content2[] = "Content for read";
    int c1len = sizeof(content1);
    int c2len = sizeof(content2);
    ASSERT_TRUE(c1len == fp1->Write(content1, c1len));
    ASSERT_TRUE(0 == fp1->Flush());
    ASSERT_TRUE(0 == fp1->Sync());
    ASSERT_TRUE(c2len == fp1->Write(content2, c2len));

    /// Close
    ASSERT_TRUE(0 == fp1->CloseFile());

    /// Rename
    if (0 == dfs->Exists(file2)) {
        ASSERT_TRUE(0 == dfs->Delete(file2));
    }
    ASSERT_TRUE(0 == dfs->Rename(file1, file2));
    ASSERT_TRUE(0 == dfs->Exists(file2));
    ASSERT_TRUE(0 != dfs->Exists(file1));

    /// GetFileSize
    uint64_t fsize = 0;
    ASSERT_TRUE(0 == dfs->GetFileSize(file2, &fsize));
    printf("c1len= %d, c2len= %d, fsize= %ld\n", c1len, c2len, fsize);
    ASSERT_TRUE(c1len + c2len == static_cast<int>(fsize));

    /// Read
    leveldb::DfsFile* fp2 = dfs->OpenFile(file2, leveldb::RDONLY);
    ASSERT_TRUE(fp2 != NULL);
    char buf[128];
    ASSERT_TRUE(c1len == fp2->Read(buf, c1len));
    ASSERT_TRUE(0 == strncmp(buf, content1, c1len));
    ASSERT_TRUE(c2len == fp2->Read(buf, c2len));
    printf("content2= %s, content_read= %s\n", content2, buf);
    ASSERT_TRUE(0 == strncmp(buf, content2, c2len));
    ASSERT_TRUE(c2len == fp2->Pread(c1len, buf, c2len));
    ASSERT_TRUE(0 == strncmp(buf, content2, c2len));
    ASSERT_TRUE(0 == fp2->CloseFile());

    /// Delete
    ASSERT_TRUE(0 == dfs->Delete(file2));
    ASSERT_TRUE(0 != dfs->Exists(file2));
}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
