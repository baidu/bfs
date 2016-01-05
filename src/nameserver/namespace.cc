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
#include "common/atomic.h"
#include "common/string_util.h"

DECLARE_string(namedb_path);
DECLARE_int64(namedb_cache_size);
DECLARE_int32(default_replica_num);

const int64_t kRootEntryid = 1;

namespace baidu {
namespace bfs {

NameSpace::NameSpace(): last_entry_id_(1) {
    leveldb::Options options;
    options.create_if_missing = true;
    options.block_cache = leveldb::NewLRUCache(FLAGS_namedb_cache_size*1024L*1024L);
    leveldb::Status s = leveldb::DB::Open(options, FLAGS_namedb_path, &db_);
    if (!s.ok()) {
        db_ = NULL;
        LOG(FATAL, "Open leveldb fail: %s\n", s.ToString().c_str());
    }
    std::string version_key(8, 0);
    version_key.append("version");
    std::string version_str;
    s = db_->Get(leveldb::ReadOptions(), version_key, &version_str);
    if (s.ok()) {
        if (version_str.size() != sizeof(int64_t)) {
            LOG(FATAL, "Bad namespace version len= %lu.", version_str.size());
        }
        version_ = *(reinterpret_cast<int64_t*>(&version_str[0]));
        LOG(INFO, "Load namespace version: %ld", version_);
    } else {
        version_ = common::timer::get_micros();
        version_str.resize(8);
        *(reinterpret_cast<int64_t*>(&version_str[0])) = version_;
        s = db_->Put(leveldb::WriteOptions(), version_key, version_str);
        if (!s.ok()) {
            LOG(FATAL, "Write namespace version to db fail: %s", s.ToString().c_str());
        }
        LOG(INFO, "Create new namespace version: %ld", version_);
    }
}

NameSpace::~NameSpace() {
    delete db_;
    db_ = NULL;
}

int64_t NameSpace::Version() const {
    return version_;
}

bool NameSpace::IsDir(int type) {
    return (type & (1<<9));
}

void NameSpace::EncodingStoreKey(int64_t entry_id,
                                 const std::string& path,
                                 std::string* key_str) {
    key_str->resize(8);
    common::util::EncodeBigEndian(&(*key_str)[0], entry_id);
    key_str->append(path);
}

bool NameSpace::GetFromStore(const std::string& key, FileInfo* info) {
    std::string value;
    leveldb::Status s = db_->Get(leveldb::ReadOptions(), key, &value);
    if (!s.ok()) {
        LOG(DEBUG, "GetFromStore get fail %s", key.substr(8).c_str());
        return false;
    }
    if (!info->ParseFromString(value)) {
        LOG(WARNING, "GetFromStore parse fail %s", key.substr(8).c_str());
        return false;
    }
    return true;
}
/// New SplitPath
/// /home/dirx/filex
///       diry/filey
/// /tmp/filez
/// 1home -> 2
/// 1tmp -> 3
/// 2dirx -> 4
/// 2diry -> 5
/// 3filez -> 6
/// 4filex -> 7
/// 5filey -> 8
bool NameSpace::LookUp(const std::string& path, FileInfo* info) {
    std::vector<std::string> paths;
    if (!common::util::SplitPath(path, &paths) || paths.empty()) {
        return false;
    }
    int64_t parent_id = kRootEntryid;
    int64_t entry_id = kRootEntryid;
    for (size_t i = 0; i < paths.size(); i++) {
        if (!LookUp(entry_id, paths[i], info)) {
            return false;
        }
        parent_id = entry_id;
        entry_id = info->entry_id();
        LOG(INFO, "LookUp %s entry_id= %ld", paths[i].c_str(), entry_id);
    }
    info->set_name(paths[paths.size()-1]);
    info->set_parent_entry_id(parent_id);
    LOG(INFO, "LookUp %s return %s", path.c_str(), info->name().c_str());
    return true;
}

bool NameSpace::LookUp(int64_t parent_id, const std::string& name, FileInfo* info) {
    std::string key_str;
    EncodingStoreKey(parent_id, name, &key_str);
    if (!GetFromStore(key_str, info)) {
        LOG(INFO, "LookUp %ld %s return false", parent_id, name.c_str());
        return false;
    }
    LOG(INFO, "LookUp %ld %s return true", parent_id, name.c_str());
    return true;
}

bool NameSpace::DeleteFileInfo(const std::string file_key) {
    leveldb::Status s = db_->Delete(leveldb::WriteOptions(), file_key);
    return s.ok();
}
bool NameSpace::UpdateFileInfo(const FileInfo& file_info) {
    std::string file_key;
    EncodingStoreKey(file_info.parent_entry_id(), file_info.name(), &file_key);
    std::string infobuf;
    file_info.SerializeToString(&infobuf);
    leveldb::Status s = db_->Put(leveldb::WriteOptions(), file_key, infobuf);
    return s.ok();
};

bool NameSpace::GetFileInfo(const std::string& path, FileInfo* file_info) {
    return LookUp(path, file_info);
}

int NameSpace::CreateFile(const std::string& path, int flags, int mode) {
    std::vector<std::string> paths;
    if (!common::util::SplitPath(path, &paths)) {
        LOG(INFO, "CreateFile split fail %s", path.c_str());
        return 886;
    }

    /// Find parent directory, create if not exist.
    FileInfo file_info;
    int64_t parent_id = kRootEntryid;
    int depth = paths.size();
    std::string info_value;
    for (int i=0; i < depth-1; ++i) {
        if (!LookUp(parent_id, paths[i], &file_info)) {
            file_info.set_type((1<<9)|0755);
            file_info.set_ctime(time(NULL));
            file_info.set_entry_id(common::atomic_add64(&last_entry_id_, 1) + 1);
            file_info.SerializeToString(&info_value);
            std::string key_str;
            EncodingStoreKey(parent_id, paths[i], &key_str);
            leveldb::Status s = db_->Put(leveldb::WriteOptions(), key_str, info_value);
            LOG(INFO, "Put %s", common::DebugString(key_str).c_str());
            assert (s.ok());
            LOG(INFO, "Create path recursively: %s %ld", paths[i].c_str(), file_info.entry_id());
        } else {
            if (!IsDir(file_info.type())) {
                LOG(WARNING, "Create path fail: %s is not a directory", paths[i].c_str());
                return 886;
            }
        }
        parent_id = file_info.entry_id();
    }

    const std::string& fname = paths[depth-1];
    if ((flags & O_TRUNC) == 0) {
        if (LookUp(parent_id, fname, &file_info)) {
            LOG(WARNING, "CreateFile %s fail: already exist!\n", fname.c_str());
            return 1;
        }
    }
    if (mode) {
        file_info.set_type(((1 << 10) - 1) & mode);
    } else {
        file_info.set_type(0755);
    }
    file_info.set_entry_id(common::atomic_add64(&last_entry_id_, 1) + 1);
    file_info.set_ctime(time(NULL));
    file_info.set_replicas(FLAGS_default_replica_num);
    //file_info.add_blocks();
    file_info.SerializeToString(&info_value);
    std::string file_key;
    EncodingStoreKey(parent_id, fname, &file_key);
    leveldb::Status s = db_->Put(leveldb::WriteOptions(), file_key, info_value);
    if (s.ok()) {
        LOG(INFO, "CreateFile %s %ld", path.c_str(), file_info.entry_id());
        return 0;
    } else {
        LOG(WARNING, "CreateFile %s fail: db put fail", path.c_str());
        return 2;
    }
}

int NameSpace::ListDirectory(const std::string& dir,
                             google::protobuf::RepeatedPtrField<FileInfo>* outputs) {
    outputs->Clear();
    std::string path = dir.empty() ? "/" : dir;
    if (path[0] != '/') {
        return 403;
    }
    if (path[path.size()-1] != '/') {
        path += '/';
    }
    int64_t entry_id = kRootEntryid;
    if (path != "/") {
        FileInfo info;
        if (!LookUp(path, &info)) {
            return 404;
        }
        entry_id = info.entry_id();
    }
    LOG(INFO, "entry_id= %ld", entry_id);
    common::timer::AutoTimer at1(100, "ListDirectory iterate", path.c_str());
    std::string key_start, key_end;
    EncodingStoreKey(entry_id, "", &key_start);
    EncodingStoreKey(entry_id + 1, "", &key_end);
    leveldb::Iterator* it = db_->NewIterator(leveldb::ReadOptions());
    for (it->Seek(key_start); it->Valid(); it->Next()) {
        leveldb::Slice key = it->key();
        if (key.compare(key_end)>=0) {
            break;
        }
        FileInfo* file_info = outputs->Add();
        bool ret = file_info->ParseFromArray(it->value().data(), it->value().size());
        assert(ret);
        file_info->set_name(std::string(key.data() + 8, key.size() - 8));
        LOG(INFO, "List %s return %s[%s]",
            path.c_str(), file_info->name().c_str(),
            common::DebugString(key.ToString()).c_str());
    }
    delete it;
    return 0;
}

int NameSpace::Rename(const std::string& old_path,
                      const std::string& new_path,
                      bool* need_unlink,
                      FileInfo* remove_file) {
    *need_unlink = false;
    FileInfo old_file;
    if (!LookUp(old_path, &old_file)) {
        LOG(WARNING, "Rename not found: %s\n", old_path.c_str());
        return 404;
    }

    std::vector<std::string> new_paths;
    if (!common::util::SplitPath(new_path, &new_paths) || new_paths.empty()) {
        LOG(INFO, "CreateFile split fail %s", new_path.c_str());
        return 886;
    }

    int64_t parent_id = kRootEntryid;
    for (uint32_t i = 0; i < new_paths.size() - 1; i++) {
        FileInfo path_file;
        if (!LookUp(parent_id, new_paths[i], &path_file)) {
            LOG(INFO, "Rename to %s which not exist", new_paths[i].c_str());
            return 404;
        }
        if (!IsDir(path_file.type())) {
            LOG(WARNING, "Rename %s to %s fail: %s is not a directory",
                old_path.c_str(), new_path.c_str(), new_paths[i].c_str());
            return 886;
        }
        parent_id = path_file.entry_id();
    }


    const std::string& dst_name = new_paths[new_paths.size() - 1];
    {
        /// dst_file maybe not exist, don't use it elsewhere.
        FileInfo dst_file;
        if (LookUp(parent_id, dst_name, &dst_file)) {
            if (IsDir(dst_file.type())) {
                LOG(INFO, "Rename %s to %s, target %o is a exist directory",
                    old_path.c_str(), new_path.c_str(), dst_file.type());
                return 403;
            }
            *need_unlink = true;
            remove_file->CopyFrom(dst_file);
        }
    }

    std::string old_key;
    EncodingStoreKey(old_file.parent_entry_id(), old_file.name(), &old_key);
    std::string new_key;
    EncodingStoreKey(parent_id, dst_name, &new_key);
    std::string value;
    old_file.clear_parent_entry_id();
    old_file.clear_name();
    old_file.SerializeToString(&value);

    // Write to persistent storage
    leveldb::WriteBatch batch;
    batch.Put(new_key, value);
    batch.Delete(old_key);
    leveldb::Status s = db_->Write(leveldb::WriteOptions(), &batch);
    if (s.ok()) {
        LOG(INFO, "Rename %s to %s[%s], replace: %d",
            old_path.c_str(), new_path.c_str(),
            common::DebugString(new_key).c_str(), *need_unlink);
        return 0;
    } else {
        LOG(WARNING, "Rename write leveldb fail: %s", old_path.c_str());
        return 1;
    }

    return 1;
}

int NameSpace::RemoveFile(const std::string& path, FileInfo* file_removed) {
    int ret_status = 0;
    if (LookUp(path, file_removed)) {
        // Only support file
        if ((file_removed->type() & (1<<9)) == 0) {
            std::string file_key;
            EncodingStoreKey(file_removed->parent_entry_id(), file_removed->name(), &file_key);
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

int NameSpace::DeleteDirectory(const std::string& path, bool recursive,
                               std::vector<FileInfo>* files_removed) {
    files_removed->clear();
    std::vector<std::string> keys;
    if (!common::util::SplitPath(path, &keys)) {
        LOG(WARNING, "Delete Directory SplitPath fail: %s\n", path.c_str());
        return 886;
    }
    FileInfo info;
    std::string store_key;
    if (!LookUp(path, &info)) {
       LOG(INFO, "Delete Directory, %s is not found.", path.c_str());
       return 404;
    } else if (!IsDir(info.type())) {
        LOG(INFO, "Delete Directory, %s is not a dir.", path.c_str());
        return 886;
    }
    return InternalDeleteDirectory(info, recursive, files_removed);
}

int NameSpace::InternalDeleteDirectory(const FileInfo& dir_info,
                                       bool recursive,
                                       std::vector<FileInfo>* files_removed) {
    int entry_id = dir_info.entry_id();
    std::string key_start, key_end;
    EncodingStoreKey(entry_id, "", &key_start);
    EncodingStoreKey(entry_id + 1, "", &key_end);

    leveldb::Iterator* it = db_->NewIterator(leveldb::ReadOptions());
    it->Seek(key_start);
    if (it->Valid() && it->key().compare(key_end) < 0 && recursive == false) {
        LOG(WARNING, "Try to delete an unempty directory unrecursively: %s",
            dir_info.name().c_str());
        delete it;
        return 886;
    } else {
        LOG(INFO, "not dir %d %s %s",
            it->Valid(), common::DebugString(it->key().ToString()).c_str(),
            common::DebugString(key_end).c_str());
    }

    int ret_status = 0;
    leveldb::WriteBatch batch;
    for (; it->Valid(); it->Next()) {
        leveldb::Slice key = it->key();
        if (key.compare(key_end) >= 0) {
            break;
        }
        std::string entry_name(key.data() + 8, key.size() - 8);
        FileInfo child_info;
        bool ret = child_info.ParseFromArray(it->value().data(), it->value().size());
        assert(ret);
        if (IsDir(child_info.type())) {
            LOG(INFO, "Recursive to path: %s", entry_name.c_str());
            ret_status = InternalDeleteDirectory(child_info, true, files_removed);
            if (ret_status != 0) {
                break;
            }
        } else {
            batch.Delete(key);
            child_info.set_name(entry_name);
            LOG(INFO, "DeleteDirectory Remove push %s", child_info.name().c_str());
            files_removed->push_back(child_info);
            LOG(INFO, "Unlink file: %s",
                std::string(key.data() + 2, key.size() - 2).c_str());
        }
    }
    delete it;

    std::string store_key;
    EncodingStoreKey(dir_info.parent_entry_id(), dir_info.name(), &store_key);
    batch.Delete(store_key);
    leveldb::Status s = db_->Write(leveldb::WriteOptions(), &batch);
    if (s.ok()) {
        LOG(INFO, "Unlink dentry done: %s[%s]",
            dir_info.name().c_str(), common::DebugString(store_key).c_str());
    } else {
        LOG(FATAL, "Namespace write to storage fail!");
        LOG(INFO, "Unlink dentry fail: %s\n", dir_info.name().c_str());
        ret_status = 886;
    }
    return ret_status;
}

bool NameSpace::RebuildBlockMap(boost::function<void (const FileInfo&)> callback) {
    leveldb::Iterator* it = db_->NewIterator(leveldb::ReadOptions());
    for (it->Seek(std::string(7, '\0') + '\1'); it->Valid(); it->Next()) {
        FileInfo file_info;
        bool ret = file_info.ParseFromArray(it->value().data(), it->value().size());
        if (last_entry_id_ < file_info.entry_id()) {
            last_entry_id_ = file_info.entry_id();
        }
        assert(ret);
        if (!IsDir(file_info.type())) {
            //a file
            callback(file_info);
        }
    }
    delete it;
    LOG(INFO, "RebuildBlockMap done. last_entry_id= E%ld", last_entry_id_);
    return true;
}

} // namespace bfs
} // namespace baidu
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
