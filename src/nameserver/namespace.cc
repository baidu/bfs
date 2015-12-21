// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#include "namespace.h"

#include <fcntl.h>
#include <gflags/gflags.h>
#include <leveldb/db.h>
#include <leveldb/cache.h>
#include <leveldb/write_batch.h>
#include <common/logging.h>

#include "common/timer.h"
#include "common/util.h"

DECLARE_string(namedb_path);
DECLARE_int64(namedb_cache_size);
DECLARE_int32(default_replica_num);

const uint32_t MAX_PATH_LENGHT = 10240;
const uint32_t MAX_PATH_DEPTH = 99;

namespace bfs {

/// 构造标准化路径
/// /home/work/file -> 00,01/home,02/home/work,03/home/work/file
bool SplitPath(const std::string& path, std::vector<std::string>* element) {
    if (path.empty() || path[0] != '/' || path.size() > MAX_PATH_LENGHT) {
        return false;
    }
    int keylen = 2;
    char keybuf[MAX_PATH_LENGHT];
    uint32_t path_depth = 0;
    int last_pos = 0;
    bool valid = true;
    for (size_t i = 0; i <= path.size(); i++) {
        if (i == path.size() || path[i] == '/') {
            if (valid) {
                if (path_depth > MAX_PATH_DEPTH) {
                    return false;
                }
                keybuf[0] = '0' + (path_depth / 10);
                keybuf[1] = '0' + (path_depth % 10);
                memcpy(keybuf + keylen, path.data() + last_pos, i - last_pos);
                keylen += i - last_pos;
                element->push_back(std::string(keybuf, keylen));
                ++path_depth;
            }
            last_pos = i;
            valid = false;
        } else {
            valid = true;
        }
    }
#if 0
    printf("SplitPath return: ");
    for (uint32_t i=0; i < element->size(); i++) {
        printf("\"%s\",", (*element)[i].c_str());
    }
    printf("\n");
#endif
    return true;
}

NameSpace::NameSpace() {
    leveldb::Options options;
    options.create_if_missing = true;
    options.block_cache = leveldb::NewLRUCache(FLAGS_namedb_cache_size*1024L*1024L);
    leveldb::Status s = leveldb::DB::Open(options, FLAGS_namedb_path, &_db);
    if (!s.ok()) {
        _db = NULL;
        LOG(FATAL, "Open leveldb fail: %s\n", s.ToString().c_str());
    }
    std::string version_key(8, '\0');
    version_key.append("version");
    std::string version_str;
    s = _db->Get(leveldb::ReadOptions(), version_key, &version_str);
    if (s.ok()) {
        if (version_str.size() != sizeof(int64_t)) {
            LOG(FATAL, "Bad namespace version len= %lu.", version_str.size());
        }
        _version = *(reinterpret_cast<int64_t*>(&version_str[0]));
        LOG(INFO, "Load namespace version: %ld", _version);
    } else {
        _version = common::timer::get_micros();
        version_str.resize(8);
        *(reinterpret_cast<int64_t*>(&version_str[0])) = _version;
        s = _db->Put(leveldb::WriteOptions(), version_key, version_str);
        if (!s.ok()) {
            LOG(FATAL, "Write namespace version to db fail: %s", s.ToString().c_str());
        }
        LOG(INFO, "Create new namespace version: %ld", _version);
    }
}

int64_t NameSpace::Version() const {
    return _version;
}

bool NameSpace::DeleteFileInfo(const std::string file_key) {
    leveldb::Status s = _db->Delete(leveldb::WriteOptions(), file_key);
    return s.ok();
}
bool NameSpace::UpdateFileInfo(const std::string& file_key, FileInfo& file_info) {
    std::string infobuf;
    file_info.SerializeToString(&infobuf);
    leveldb::Status s = _db->Put(leveldb::WriteOptions(), file_key, infobuf);
    return s.ok();
};

bool NameSpace::GetFileInfo(const std::string& path,
                            FileInfo* file_info,
                            std::string* file_key) {
    std::vector<std::string> keys;
    if (!SplitPath(path, &keys)) {
        LOG(WARNING, "Unlink SplitPath fail: %s\n", path.c_str());
        return false;
    }
    std::string infobuf;
    *file_key = *keys.rbegin();
    leveldb::Status s = _db->Get(leveldb::ReadOptions(), *file_key, &infobuf);
    if (!s.ok()) {
        return false;
    }
    
    bool ret = file_info->ParseFromString(infobuf);
    assert(ret);
    return file_info;
    /*
    int64_t parent_id = 1;
    FileInfo info;
    for (uint32_t i = 0; i < keys.size(); i++) {
        std::string db_key(4, 0);
        *reinterpret_cast<int*>(&db_key[i]) = 1;
        db_key.append(keys[i]);
        std::string value;
        leveldb::Status s = _db.Get(db_key, &value);
        if (!s.ok()) {
            return NULL;
        }
        if (!info.ParseFromString(value)) {
            return NULL;
        }
        parent_id = info.id();
    }
    return new FileInfo(info);*/
}

int NameSpace::CreateFile(const std::string& file_name, int flags, int mode) {
    std::vector<std::string> file_keys;
    if (!SplitPath(file_name, &file_keys)) {
        return 886;
    }

    /// Find parent directory, create if not exist.
    FileInfo file_info;
    std::string info_value;
    int depth = file_keys.size();
    leveldb::Status s;
    for (int i=0; i < depth-1; ++i) {
        s = _db->Get(leveldb::ReadOptions(), file_keys[i], &info_value);
        if (s.IsNotFound()) {
            file_info.set_type((1<<9)|0755);
            file_info.set_ctime(time(NULL));
            file_info.SerializeToString(&info_value);
            s = _db->Put(leveldb::WriteOptions(), file_keys[i], info_value);
            assert (s.ok());
            LOG(INFO, "Create path recursively: %s\n",file_keys[i].c_str()+2);
        } else {
            bool ret = file_info.ParseFromString(info_value);
            assert(ret);
            if ((file_info.type() & (1<<9)) == 0) {
                LOG(WARNING, "Create path fail: %s is not a directory\n", file_keys[i].c_str() + 2);
                return 886;
            }
        }
    }
    
    const std::string& file_key = file_keys[depth-1];
    if ((flags & O_TRUNC) == 0) {
        s = _db->Get(leveldb::ReadOptions(), file_key, &info_value);
        if (s.ok()) {
            LOG(WARNING, "CreateFile %s fail: already exist!\n", file_name.c_str());
            return 1;
        }
    }
    if (mode) {
        file_info.set_type(((1 << 10) - 1) & mode);
    } else {
        file_info.set_type(755);
    }
    file_info.set_id(0);
    file_info.set_ctime(time(NULL));
    file_info.set_replicas(FLAGS_default_replica_num);
    //file_info.add_blocks();
    file_info.SerializeToString(&info_value);
    s = _db->Put(leveldb::WriteOptions(), file_key, info_value);
    if (s.ok()) {
        LOG(INFO, "CreateFile %s\n", file_key.c_str());
        return 0;
    } else {
        LOG(WARNING, "CreateFile %s fail: db put fail", file_key.c_str());
        return 2;
    }
}

int NameSpace::ListDirectory(const std::string& dir,
                             google::protobuf::RepeatedPtrField<FileInfo>* outputs) {
    std::string path = dir;
    if (path.empty() || path[0] != '/') {
        path = "/";
    }
    if (path[path.size()-1] != '/') {
        path += '/';
    }
    ///TODO: Check path existent

    path += "#";
    std::vector<std::string> keys;
    if (!SplitPath(path, &keys)) {
        LOG(WARNING, "SplitPath fail: %s\n", path.c_str());
        return 886;
    }

    const std::string& file_start_key = keys[keys.size()-1];
    std::string file_end_key = file_start_key;
    if (file_end_key[file_end_key.size()-1] == '#') {
        file_end_key[file_end_key.size()-1] = '\255';
    } else {
        file_end_key += "#";
    }

    common::timer::AutoTimer at1(100, "ListDirectory iterate", path.c_str());
    //printf("List Directory: %s, return: ", file_start_key.c_str());
    leveldb::Iterator* it = _db->NewIterator(leveldb::ReadOptions());
    for (it->Seek(file_start_key); it->Valid(); it->Next()) {
        leveldb::Slice key = it->key();
        if (key.compare(file_end_key)>=0) {
            break;
        }
        FileInfo* file_info = outputs->Add();
        bool ret = file_info->ParseFromArray(it->value().data(), it->value().size());
        assert(ret);
        file_info->set_name(key.data()+2, it->key().size()-2);
    }
    delete it;
    return 0;
}

int NameSpace::Rename(const std::string& old_path,
                      const std::string& new_path,
                      bool* need_unlink,
                      FileInfo* remove_file) {
    FileInfo old_file;
    std::string old_key;
    if (!GetFileInfo(old_path, &old_file, &old_key)) {
        LOG(WARNING, "Rename not found: %s\n", old_path.c_str());
        return 404;
    }

    FileInfo new_file;
    std::string new_key;
    *need_unlink = GetFileInfo(new_path, &new_file, &new_key);

    // Directory rename is not impliment.
    if ((old_file.type() & (1<<9)) == 0) {
        leveldb::WriteBatch batch;
        std::string value;
        old_file.SerializeToString(&value);
        batch.Put(new_key, value);
        batch.Delete(old_key);
        leveldb::Status s = _db->Write(leveldb::WriteOptions(), &batch);
        if (s.ok()) {
            LOG(INFO, "Rename %s to %s, replace: %d",
                old_path.c_str(), new_path.c_str(), need_unlink);
            return 0;
        } else {
            LOG(WARNING, "Rename write leveldb fail: %s", old_path.c_str());
            return 1;
        }
    } else {
        LOG(WARNING, "Rename not support directory: %s", old_path.c_str());
        return 403;
    }
    return 1;
}

int NameSpace::RemoveFile(const std::string& path, FileInfo* file_removed) {
    int ret_status = 0;
    std::string file_key;
    if (GetFileInfo(path, file_removed, &file_key)) {
        // Only support file
        if ((file_removed->type() & (1<<9)) == 0) {
            if (DeleteFileInfo(file_key)) {
                LOG(INFO, "Unlink done: %s\n", path.c_str());
                ret_status = 0;
            } else {
                LOG(WARNING, "Unlink write meta fail: %s\n", path.c_str());
                ret_status = 400;
            }
        } else {
            LOG(WARNING, "Unlink not support directory: %s\n", path.c_str());
            ret_status = 403;
        }
    } else {
        LOG(WARNING, "Unlink not found: %s\n", path.c_str());
        ret_status = 404;
    }
    return ret_status;
}

int NameSpace::DeleteDirectory(std::string& path, bool recursive,
                               std::vector<FileInfo>* files_removed) {
    int ret_status = 0;
    std::vector<std::string> keys;

    if (!SplitPath(path, &keys)) {
        LOG(WARNING, "Delete Directory SplitPath fail: %s\n", path.c_str());
        return 886;
    }
    std::string dentry_key = keys[keys.size() - 1];
    {
        std::string value;
        leveldb::Status s = _db->Get(leveldb::ReadOptions(), dentry_key, &value);
        if (!s.ok()) {
            LOG(INFO, "Delete Directory, %s is not found.", dentry_key.data() + 2);
            return 404;
        }
    }
    
    keys.clear();
    if (path[path.size() - 1] != '/') {
        path += '/';
    }
    path += '#';

    if (!SplitPath(path, &keys)) {
        LOG(WARNING, "Delete Directory SplitPath fail: %s\n", path.c_str());
        ret_status = 886;
        return ret_status;
    }
    const std::string& file_start_key = keys[keys.size() - 1];
    std::string file_end_key = file_start_key;
    if (file_end_key[file_end_key.size() - 1] == '#') {
        file_end_key[file_end_key.size() - 1] = '\255';
    } else {
        file_end_key += '#';
    }

    leveldb::Iterator* it = _db->NewIterator(leveldb::ReadOptions());
    it->Seek(file_start_key);
    if (it->Valid() && recursive == false) {
        LOG(WARNING, "Try to delete an unempty directory unrecursively: %s\n", dentry_key.c_str());
        delete it;
        ret_status = 886;
        return ret_status;
    }

    for (; it->Valid(); it->Next()) {
        leveldb::Slice key = it->key();
        if (key.compare(file_end_key) >= 0) {
            break;
        }
        FileInfo file_info;
        bool ret = file_info.ParseFromArray(it->value().data(), it->value().size());
        assert(ret);
        if ((file_info.type() & (1 << 9)) != 0) {
            std::string dir_path(std::string(key.data() + 2, key.size() - 2));
            LOG(INFO, "Recursive to path: %s\n", dir_path.c_str());
            ret_status = DeleteDirectory(dir_path, recursive, files_removed);
            if (ret_status != 0) {
                break;
            }
        } else {
            leveldb::Status s = _db->Delete(leveldb::WriteOptions(),
                                            std::string(key.data(),
                                            key.size()));
            if (s.ok()) {
                files_removed->push_back(file_info);
                LOG(INFO, "Unlink file done: %s",
                    std::string(key.data() + 2, key.size() - 2).c_str());
            } else {
                LOG(WARNING, "Unlink file fail: %s",
                    std::string(key.data() + 2, key.size() - 2).c_str());
                ret_status = 886;
                break;
            }
        }
    }

    if (ret_status == 0) {
        leveldb::Status s = _db->Delete(leveldb::WriteOptions(), dentry_key);
        if (s.ok()) {
            LOG(INFO, "Unlink dentry done: %s\n", dentry_key.c_str() + 2);
        } else {
            LOG(INFO, "Unlink dentry fail: %s\n", dentry_key.c_str() + 2);
            ret_status = 886;
        }
    }
    delete it;
    return ret_status;
}
leveldb::Iterator* NameSpace::NewIterator() {
    return _db->NewIterator(leveldb::ReadOptions());
}

}
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
