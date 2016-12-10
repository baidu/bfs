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
#include <common/timer.h>
#include <common/util.h>
#include <common/atomic.h>
#include <common/string_util.h>

#include "nameserver/sync.h"

DECLARE_string(namedb_path);
DECLARE_int64(namedb_cache_size);
DECLARE_int32(default_replica_num);
DECLARE_int32(block_id_allocation_size);
DECLARE_bool(check_orphan);

const int64_t kRootEntryid = 1;


namespace baidu {
namespace bfs {

NameSpace::NameSpace(bool standalone): version_(0), last_entry_id_(1),
    block_id_upbound_(1), next_block_id_(1) {
    leveldb::Options options;
    options.create_if_missing = true;
    options.block_cache = leveldb::NewLRUCache(FLAGS_namedb_cache_size * 1024L * 1024L);
    leveldb::Status s = leveldb::DB::Open(options, FLAGS_namedb_path, &db_);
    if (!s.ok()) {
        db_ = NULL;
        LOG(FATAL, "Open leveldb fail: %s\n", s.ToString().c_str());
        return;
    }
    if (standalone) {
        Activate(NULL, NULL);
    }
}

void NameSpace::Activate(std::function<void (const FileInfo&)> callback, NameServerLog* log) {
    std::string version_key(8, 0);
    version_key.append("version");
    std::string version_str;
    leveldb::Status s = db_->Get(leveldb::ReadOptions(), version_key, &version_str);
    if (s.ok()) {
        if (version_str.size() != sizeof(int64_t)) {
            LOG(FATAL, "Bad namespace version len= %lu.", version_str.size());
        }
        version_ = *(reinterpret_cast<int64_t*>(&version_str[0]));
        LOG(INFO, "Load namespace version: %ld ", version_);
    } else {
        version_ = common::timer::get_micros();
        version_str.resize(8);
        *(reinterpret_cast<int64_t*>(&version_str[0])) = version_;

        leveldb::Status s = db_->Put(leveldb::WriteOptions(), version_key, version_str);
        if (!s.ok()) {
            LOG(FATAL, "Write NameSpace version failed %s", s.ToString().c_str());
        }
        EncodeLog(log, kSyncWrite, version_key, version_str);
        LOG(INFO, "Create new namespace version: %ld ", version_);
    }
    SetupRoot();
    RebuildBlockMap(callback);
    InitBlockIdUpbound(log);
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
    common::util::EncodeBigEndian(&(*key_str)[0], (uint64_t)entry_id);
    key_str->append(path);
}

void NameSpace::DecodingStoreKey(const std::string& key_str,
                                 int64_t* entry_id,
                                 std::string* path) {
    assert(key_str.size() >= 8UL);
    if (entry_id) {
        *entry_id = common::util::DecodeBigEndian64(key_str.c_str());
    }
    if (path) {
        path->assign(key_str, 8, std::string::npos);
    }
}

bool NameSpace::GetFromStore(const std::string& key, FileInfo* info) {
    std::string value;
    leveldb::Status s = db_->Get(leveldb::ReadOptions(), key, &value);
    if (!s.ok()) {
        LOG(DEBUG, "GetFromStore get fail %s %s",
            key.substr(8).c_str(), s.ToString().c_str());
        return false;
    }
    if (!info->ParseFromString(value)) {
        LOG(WARNING, "GetFromStore parse fail %s", key.substr(8).c_str());
        return false;
    }
    return true;
}

void NameSpace::SetupRoot() {
    root_path_.set_entry_id(kRootEntryid);
    root_path_.set_name("");
    root_path_.set_parent_entry_id(kRootEntryid);
    root_path_.set_type(01755);
    root_path_.set_ctime(static_cast<uint32_t>(version_ / 1000000));
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
    if (path == "/") {
        info->CopyFrom(root_path_);
        return true;
    }
    std::vector<std::string> paths;
    if (!common::util::SplitPath(path, &paths) || path.empty()) {
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
        LOG(DEBUG, "LookUp %s entry_id= E%ld ", paths[i].c_str(), entry_id);
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
        LOG(INFO, "LookUp E%ld %s return false", parent_id, name.c_str());
        return false;
    }
    LOG(DEBUG, "LookUp E%ld %s return true", parent_id, name.c_str());
    return true;
}

bool NameSpace::DeleteFileInfo(const std::string file_key, NameServerLog* log) {
    leveldb::Status s = db_->Delete(leveldb::WriteOptions(), file_key);
    if (!s.ok()) {
        return false;
    }
    EncodeLog(log,kSyncDelete, file_key, "");
    return true;
}
bool NameSpace::UpdateFileInfo(const FileInfo& file_info, NameServerLog* log) {
    {
        MutexLock lock(&mu_);
        for (auto i = 0; i < file_info.blocks_size(); ++i) {
            if (file_info.blocks(i) >= block_id_upbound_) {
                UpdateBlockIdUpbound(log);
            }
        }
    }

    FileInfo file_info_for_ldb;
    file_info_for_ldb.CopyFrom(file_info);
    file_info_for_ldb.clear_cs_addrs();

    std::string file_key;
    EncodingStoreKey(file_info_for_ldb.parent_entry_id(), file_info_for_ldb.name(), &file_key);
    std::string infobuf_for_ldb, infobuf_for_sync;
    file_info_for_ldb.SerializeToString(&infobuf_for_ldb);
    file_info.SerializeToString(&infobuf_for_sync);

    leveldb::Status s = db_->Put(leveldb::WriteOptions(), file_key, infobuf_for_ldb);
    if (!s.ok()) {
        LOG(WARNING, "NameSpace write to db fail: %s", s.ToString().c_str());
        return false;
    }
    EncodeLog(log, kSyncWrite, file_key, infobuf_for_ldb);
    return true;
};

bool NameSpace::GetFileInfo(const std::string& path, FileInfo* file_info) {
    return LookUp(path, file_info);
}

StatusCode NameSpace::CreateFile(const std::string& path, int flags, int mode, int replica_num,
                                 std::vector<int64_t>* blocks_to_remove,
                                 NameServerLog* log) {
    std::vector<std::string> paths;
    if (!common::util::SplitPath(path, &paths)) {
        LOG(INFO, "CreateFile split fail %s", path.c_str());
        return kBadParameter;
    }

    /// Find parent directory, create if not exist.
    FileInfo file_info;
    int64_t parent_id = kRootEntryid;
    int depth = paths.size();
    std::string info_value;
    for (int i = 0; i < depth - 1; ++i) {
        if (!LookUp(parent_id, paths[i], &file_info)) {
            file_info.set_type((1 << 9) | 0755);
            file_info.set_ctime(time(NULL));
            file_info.set_entry_id(common::atomic_add64(&last_entry_id_, 1) + 1);
            file_info.SerializeToString(&info_value);
            std::string key_str;
            EncodingStoreKey(parent_id, paths[i], &key_str);
            leveldb::Status s = db_->Put(leveldb::WriteOptions(), key_str, info_value);
            assert(s.ok());
            EncodeLog(log, kSyncWrite, key_str, info_value);
            LOG(INFO, "Create path recursively: %s E%ld ", paths[i].c_str(), file_info.entry_id());
        } else {
            if (!IsDir(file_info.type())) {
                LOG(INFO, "Create path fail: %s is not a directory", paths[i].c_str());
                return kBadParameter;
            }
        }
        parent_id = file_info.entry_id();
    }

    const std::string& fname = paths[depth - 1];
    bool exist = LookUp(parent_id, fname, &file_info);
    if (exist) {
        if ((flags & O_TRUNC) == 0) {
            LOG(INFO, "CreateFile %s fail: already exist!", fname.c_str());
            return kFileExists;
        } else {
            if (IsDir(file_info.type())) {
                LOG(INFO, "CreateFile %s fail: directory with same name exist", fname.c_str());
                return kFileExists;
            }
            for (int i = 0; i < file_info.blocks_size(); i++) {
                blocks_to_remove->push_back(file_info.blocks(i));
            }
        }
    }
    file_info.set_type(((1 << 10) - 1) & mode);
    file_info.set_entry_id(common::atomic_add64(&last_entry_id_, 1) + 1);
    file_info.set_ctime(time(NULL));
    file_info.set_replicas(replica_num <= 0 ? FLAGS_default_replica_num : replica_num);
    //file_info.add_blocks();
    file_info.SerializeToString(&info_value);
    std::string file_key;
    EncodingStoreKey(parent_id, fname, &file_key);
    leveldb::Status s = db_->Put(leveldb::WriteOptions(), file_key, info_value);
    if (s.ok()) {
        LOG(INFO, "CreateFile %s E%ld ", path.c_str(), file_info.entry_id());
        EncodeLog(log, kSyncWrite, file_key, info_value);
        return kOK;
    } else {
        LOG(WARNING, "CreateFile %s fail: db put fail %s", path.c_str(), s.ToString().c_str());
        return kUpdateError;
    }
}

StatusCode NameSpace::ListDirectory(const std::string& path,
                             google::protobuf::RepeatedPtrField<FileInfo>* outputs) {
    outputs->Clear();
    FileInfo info;
    if (!LookUp(path, &info)) {
        return kNsNotFound;
    }
    if (!IsDir(info.type())) {
        FileInfo* file_info = outputs->Add();
        file_info->CopyFrom(info);
        //for a file, name should be empty because it's a relative path
        file_info->clear_name();
        LOG(INFO, "List %s return %ld items", path.c_str(), outputs->size());
        return kOK;
    }
    int64_t entry_id = info.entry_id();
    LOG(DEBUG, "ListDirectory entry_id= E%ld ", entry_id);
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
        LOG(DEBUG, "List %s return %s[%s]",
            path.c_str(), file_info->name().c_str(),
            common::DebugString(key.ToString()).c_str());
    }
    LOG(INFO, "List return %ld items", outputs->size());
    delete it;
    return kOK;
}

StatusCode NameSpace::Rename(const std::string& old_path,
                      const std::string& new_path,
                      bool* need_unlink,
                      FileInfo* remove_file,
                      NameServerLog* log) {
    *need_unlink = false;
    if (old_path == "/" || new_path == "/" || old_path == new_path) {
        return kBadParameter;
    }
    FileInfo old_file;
    if (!LookUp(old_path, &old_file)) {
        LOG(INFO, "Rename not found: %s\n", old_path.c_str());
        return kNsNotFound;
    }

    std::vector<std::string> new_paths;
    if (!common::util::SplitPath(new_path, &new_paths) || new_paths.empty()) {
        LOG(INFO, "CreateFile split fail %s", new_path.c_str());
        return kBadParameter;
    }

    int64_t parent_id = kRootEntryid;
    for (uint32_t i = 0; i < new_paths.size() - 1; i++) {
        FileInfo path_file;
        if (!LookUp(parent_id, new_paths[i], &path_file)) {
            LOG(INFO, "Rename to %s which not exist", new_paths[i].c_str());
            return kNsNotFound;
        }
        if (!IsDir(path_file.type())) {
            LOG(INFO, "Rename %s to %s fail: %s is not a directory",
                old_path.c_str(), new_path.c_str(), new_paths[i].c_str());
            return kBadParameter;
        }
        if (path_file.entry_id() == old_file.entry_id()) {
            LOG(INFO, "Rename %s to %s fail: %s is the parent directory of %s",
                    old_path.c_str(), new_path.c_str(), old_path.c_str(),
                    new_path.c_str());
            return kBadParameter;
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
                return kTargetDirExists;
            }
            *need_unlink = true;
            remove_file->CopyFrom(dst_file);
            remove_file->set_name(dst_name);
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

    EncodeLog(log, kSyncWrite, new_key, value);
    EncodeLog(log, kSyncDelete, old_key, "");

    leveldb::Status s = db_->Write(leveldb::WriteOptions(), &batch);
    if (s.ok()) {
        LOG(INFO, "Rename %s to %s[%s], replace: %d",
            old_path.c_str(), new_path.c_str(),
            common::DebugString(new_key).c_str(), *need_unlink);
        return kOK;
    } else {
        LOG(WARNING, "Rename write leveldb fail: %s %s", old_path.c_str(), s.ToString().c_str());
        return kUpdateError;
    }
}

StatusCode NameSpace::RemoveFile(const std::string& path, FileInfo* file_removed, NameServerLog* log) {
    StatusCode ret_status = kOK;
    if (LookUp(path, file_removed)) {
        // Only support file
        if ((file_removed->type() & (1<<9)) == 0) {
            if (path == "/" || path.empty()) {
                LOG(INFO, "root type= %d", file_removed->type());
            }
            std::string file_key;
            EncodingStoreKey(file_removed->parent_entry_id(), file_removed->name(), &file_key);
            if (DeleteFileInfo(file_key, log)) {
                LOG(INFO, "Unlink done: %s\n", path.c_str());
                ret_status = kOK;
            } else {
                LOG(WARNING, "Unlink write meta fail: %s\n", path.c_str());
                ret_status = kUpdateError;
            }
        } else {
            LOG(INFO, "Unlink not support directory: %s\n", path.c_str());
            ret_status = kBadParameter;
        }
    } else {
        LOG(INFO, "Unlink not found: %s\n", path.c_str());
        ret_status = kNsNotFound;
    }
    return ret_status;
}

StatusCode NameSpace::DiskUsage(const std::string& path, uint64_t* du_size) {
    if (!du_size) {
        return kOK;
    }
    *du_size = 0;
    FileInfo info;
    if (!LookUp(path, &info)) {
        LOG(INFO, "Du Directory or File, %s is not found.", path.c_str());
        return kNsNotFound;
    } else if (!IsDir(info.type())) {
        *du_size = info.size();
        return kOK;
    }
    return InternalComputeDiskUsage(info, du_size);
}

StatusCode NameSpace::InternalComputeDiskUsage(const FileInfo& info, uint64_t* du_size) {
    int64_t entry_id = info.entry_id();
    std::string key_start, key_end;
    EncodingStoreKey(entry_id, "", &key_start);
    EncodingStoreKey(entry_id + 1, "", &key_end);
    leveldb::Iterator* it = db_->NewIterator(leveldb::ReadOptions());
    it->Seek(key_start);

    StatusCode ret_status = kOK;
    for (; it->Valid(); it->Next()) {
        leveldb::Slice key = it->key();
        if (key.compare(key_end) >= 0) {
            break;
        }
        std::string entry_name(key.data() + 8, key.size() - 8);
        FileInfo child_info;
        assert(child_info.ParseFromArray(it->value().data(), it->value().size()));
        if (IsDir(child_info.type())) {
            child_info.set_parent_entry_id(entry_id);
            ret_status = InternalComputeDiskUsage(child_info, du_size);
            if (ret_status != kOK) {
                break;
            }
        } else {
            //LOG(DEBUG, "Compute Disk Usage: E%ld, file %s, size %lu", entry_id, entry_name.c_str(), child_info.size());
            *du_size += child_info.size();
        }
    }
    delete it;
    return ret_status;
}

StatusCode NameSpace::DeleteDirectory(const std::string& path, bool recursive,
                               std::vector<FileInfo>* files_removed, NameServerLog* log) {
    files_removed->clear();
    FileInfo info;
    if (!LookUp(path, &info)) {
        LOG(INFO, "Delete Directory, %s is not found.", path.c_str());
        return kNsNotFound;
    } else if (!IsDir(info.type())) {
        LOG(INFO, "Delete Directory, %s %d is not a dir.", path.c_str(), info.type());
        return kBadParameter;
    }
    return InternalDeleteDirectory(info, recursive, files_removed, log);
}

StatusCode NameSpace::InternalDeleteDirectory(const FileInfo& dir_info,
                                       bool recursive,
                                       std::vector<FileInfo>* files_removed,
                                       NameServerLog* log) {
    int64_t entry_id = dir_info.entry_id();
    std::string key_start, key_end;
    EncodingStoreKey(entry_id, "", &key_start);
    EncodingStoreKey(entry_id + 1, "", &key_end);

    leveldb::Iterator* it = db_->NewIterator(leveldb::ReadOptions());
    it->Seek(key_start);
    if (it->Valid() && it->key().compare(key_end) < 0 && recursive == false) {
        LOG(INFO, "Try to delete an unempty directory unrecursively: %s",
            dir_info.name().c_str());
        delete it;
        return kDirNotEmpty;
    }

    StatusCode ret_status = kOK;
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
            child_info.set_parent_entry_id(entry_id);
            child_info.set_name(entry_name);
            LOG(INFO, "Recursive to path: %s", entry_name.c_str());
            ret_status = InternalDeleteDirectory(child_info, true, files_removed, log);
            if (ret_status != kOK) {
                break;
            }
        } else {
            EncodeLog(log, kSyncDelete, std::string(key.data(), key.size()), "");
            batch.Delete(key);
            child_info.set_parent_entry_id(entry_id);
            child_info.set_name(entry_name);
            LOG(DEBUG, "DeleteDirectory Remove push %s", entry_name.c_str());
            files_removed->push_back(child_info);
            LOG(INFO, "Unlink file: %s", entry_name.c_str());
        }
    }
    delete it;

    std::string store_key;
    EncodingStoreKey(dir_info.parent_entry_id(), dir_info.name(), &store_key);
    batch.Delete(store_key);
    EncodeLog(log, kSyncDelete, store_key, "");

    leveldb::Status s = db_->Write(leveldb::WriteOptions(), &batch);
    if (s.ok()) {
        LOG(INFO, "Delete directory done: %s[%s]",
            dir_info.name().c_str(), common::DebugString(store_key).c_str());
    } else {
        LOG(INFO, "Unlink dentry fail: %s\n", dir_info.name().c_str());
        LOG(FATAL, "Namespace write to storage fail!");
        ret_status = kUpdateError;
    }
    return ret_status;
}

bool NameSpace::RebuildBlockMap(std::function<void (const FileInfo&)> callback) {
    int64_t block_num = 0;
    int64_t file_num = 0;
    std::set<int64_t> entry_id_set;
    entry_id_set.insert(root_path_.entry_id());
    leveldb::Iterator* it = db_->NewIterator(leveldb::ReadOptions());
    for (it->Seek(std::string(7, '\0') + '\1'); it->Valid(); it->Next()) {
        FileInfo file_info;
        bool ret = file_info.ParseFromArray(it->value().data(), it->value().size());
        assert(ret);
        if (last_entry_id_ < file_info.entry_id()) {
            last_entry_id_ = file_info.entry_id();
        }
        if (!IsDir(file_info.type())) {
            //a file
            for (int i = 0; i < file_info.blocks_size(); i++) {
                if (file_info.blocks(i) >= next_block_id_) {
                    next_block_id_ = file_info.blocks(i) + 1;
                    block_id_upbound_ = next_block_id_;
                }
                ++block_num;
            }
            ++file_num;
            if (callback) {
                callback(file_info);
            }
        } else {
            entry_id_set.insert(file_info.entry_id());
        }
    }
    LOG(INFO, "RebuildBlockMap done. %ld directories, %ld files, "
              "%lu blocks, last_entry_id= E%ld",
        entry_id_set.size(), file_num, block_num, last_entry_id_);
    if (FLAGS_check_orphan) {
        std::vector<std::pair<std::string, std::string> > orphan_entrys;
        for (it->Seek(std::string(7, '\0') + '\1'); it->Valid(); it->Next()) {
            FileInfo file_info;
            bool ret = file_info.ParseFromArray(it->value().data(), it->value().size());
            assert(ret);
            int64_t parent_entry_id = 0;
            std::string filename;
            DecodingStoreKey(it->key().ToString(), &parent_entry_id, &filename);
            if (entry_id_set.find(parent_entry_id) == entry_id_set.end()) {
                LOG(WARNING, "Orphan entry PE%ld E%ld %s",
                    parent_entry_id, file_info.entry_id(), filename.c_str());
                orphan_entrys.push_back(std::make_pair(it->key().ToString(),
                                                       it->value().ToString()));
            }
        }
        LOG(INFO, "Check orphan done, %lu entries", orphan_entrys.size());
    }
    delete it;
    return true;
}

std::string NameSpace::NormalizePath(const std::string& path) {
    // Is there a better implementation?
    std::string ret;
    if (path.empty() || path[0] != '/') {
        ret = "/";
    }
    bool slash = false;
    for (uint32_t i = 0; i < path.size(); i++) {
        if (path[i] == '/') {
            if (slash) continue;
            slash = true;
        } else {
            slash = false;
        }
        ret.push_back(path[i]);
    }
    if (ret.size() > 1U && ret[ret.size() - 1] == '/') {
        ret.resize(ret.size() - 1);
    }
    return ret;
}

void NameSpace::TailLog(const std::string& logstr) {
    NameServerLog log;
    if(!log.ParseFromString(logstr)) {
        LOG(FATAL, "Parse log fail: %s", common::DebugString(logstr).c_str());
    }
    for (int i = 0; i < log.entries_size(); i++) {
        const NsLogEntry& entry = log.entries(i);
        int type = entry.type();
        leveldb::Status s;
        if (type == kSyncWrite) {
            s = db_->Put(leveldb::WriteOptions(), entry.key(), entry.value());
        } else if (type == kSyncDelete) {
            s = db_->Delete(leveldb::WriteOptions(), entry.key());
        }
        if (!s.ok()) {
            LOG(FATAL, "TailLog failed");
        }
    }
}

uint32_t NameSpace::EncodeLog(NameServerLog* log, int32_t type,
                              const std::string& key, const std::string& value) {
    if (log == NULL) {
        return 0;
    }
    NsLogEntry* entry = log->add_entries();
    entry->set_type(type);
    entry->set_key(key);
    entry->set_value(value);
    return entry->ByteSize();
}
void NameSpace::UpdateBlockIdUpbound(NameServerLog* log) {
    std::string block_id_upbound_key(8, 0);
    block_id_upbound_key.append("block_id_upbound");
    std::string block_id_upbound_str;
    block_id_upbound_str.resize(8);
    block_id_upbound_ += FLAGS_block_id_allocation_size;
    *(reinterpret_cast<int64_t*>(&block_id_upbound_str[0])) = block_id_upbound_;
    leveldb::Status s = db_->Put(leveldb::WriteOptions(), block_id_upbound_key, block_id_upbound_str);
    if (!s.ok()) {
        LOG(FATAL, "Update block id upbound fail: %s", s.ToString().c_str());
    } else {
        LOG(INFO, "Update block id upbound to %ld", block_id_upbound_);
    }
    EncodeLog(log, kSyncWrite, block_id_upbound_key, block_id_upbound_str);
}
void NameSpace::InitBlockIdUpbound(NameServerLog* log) {
    std::string block_id_upbound_key(8, 0);
    block_id_upbound_key.append("block_id_upbound");
    std::string block_id_upbound_str;
    leveldb::Status s = db_->Get(leveldb::ReadOptions(), block_id_upbound_key, &block_id_upbound_str);
    if (s.IsNotFound()) {
        LOG(INFO, "Init block id upbound");
        UpdateBlockIdUpbound(log);
    } else if (s.ok()) {
        block_id_upbound_ = *(reinterpret_cast<int64_t*>(&block_id_upbound_str[0]));
        next_block_id_ = block_id_upbound_;
        LOG(INFO, "Load block id upbound: %ld", block_id_upbound_);
        UpdateBlockIdUpbound(log);
    } else {
        LOG(FATAL, "Load block id upbound failed: %s", s.ToString().c_str());
    }
}

int64_t NameSpace::GetNewBlockId() {
    MutexLock lock(&mu_);
    return next_block_id_++;
}

} // namespace bfs
} // namespace baidu
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
