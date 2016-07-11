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
#include <boost/bind.hpp>

#include "nameserver/sync.h"

DECLARE_string(namedb_path);
DECLARE_int64(namedb_cache_size);
DECLARE_int32(default_replica_num);
DECLARE_int32(block_id_allocation_size);

const int64_t kRootEntryid = 1;


namespace baidu {
namespace bfs {

SyncSnapshot::SyncSnapshot(leveldb::DB* db) : db_(db) {}

void SyncSnapshot::Add(int64_t index) {
    MutexLock lock(&mu_);
    std::map<int64_t, SS*>::iterator it = snapshots_.find(index);
    if (it != snapshots_.end()) {
        // logdb does not allow duplicate index
        LOG(FATAL, "SyncSnapshot Add failed %ld ", index);
    }
    SS* ss = new SS(db_->GetSnapshot());
    snapshots_.insert(std::make_pair(index, ss)).second;
    ss->ref.Inc();
}

bool SyncSnapshot::Get(const leveldb::Snapshot** s, int64_t* index) {
    MutexLock lock(&mu_);
    if (snapshots_.empty()) {
        return false;
    }
    std::map<int64_t, SS*>::iterator it = snapshots_.begin();
    *index = it->first;
    SS* ss = it->second;
    *s = ss->snapshot;
    ss->ref.Inc();
    return true;
}

void SyncSnapshot::Release(int64_t index) {
    if (index == -1) return;
    MutexLock lock(&mu_);
    std::map<int64_t, SS*>::iterator it = snapshots_.find(index);
    if (it == snapshots_.end()) {
        LOG(INFO, "SyncSnapshot Release %ld failed, not found", index);
        return;
    }
    SS* ss = it->second;
    ss->ref.Dec();
    if (ss->ref.Get() == 0) {
        LOG(INFO, "SyncSnapshot Release %ld ", index);
        db_->ReleaseSnapshot(ss->snapshot);
        delete ss;
        snapshots_.erase(it);
    }
}

NameSpace::NameSpace(bool standalone): version_(0), last_entry_id_(1),
    block_id_upbound_(0), next_block_id_(block_id_upbound_ + 1) {
    leveldb::Options options;
    options.create_if_missing = true;
    options.block_cache = leveldb::NewLRUCache(FLAGS_namedb_cache_size*1024L*1024L);
    leveldb::Status s = leveldb::DB::Open(options, FLAGS_namedb_path, &db_);
    if (!s.ok()) {
        db_ = NULL;
        LOG(FATAL, "Open leveldb fail: %s\n", s.ToString().c_str());
        return;
    }
    sync_snapshots_ = new SyncSnapshot(db_);
    if (standalone) {
        Activate(NULL);
    }
}

void NameSpace::Activate(NameServerLog* log) {
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
    std::string block_id_upbound_key(8, 0);
    block_id_upbound_key.append("block_id_upbound");
    std::string block_id_upbound_str;
    s = db_->Get(leveldb::ReadOptions(), block_id_upbound_key, &block_id_upbound_str);
    if (s.IsNotFound()) {
        LOG(INFO, "Init block id upbound");
        UpdateBlockIdUpbound(log);
    } else if (s.ok()) {
        block_id_upbound_ = *(reinterpret_cast<int64_t*>(&block_id_upbound_str[0]));
        LOG(INFO, "Load block id upbound: %ld", block_id_upbound_);
        UpdateBlockIdUpbound(log);
    } else {
        LOG(FATAL, "Load block id upbound failed: %s", s.ToString().c_str());
    }
    SetupRoot();
}
NameSpace::~NameSpace() {
    delete db_;
    delete sync_snapshots_;
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
    int64_t seq = -1;
    const leveldb::Snapshot* s;
    leveldb::ReadOptions read_option = leveldb::ReadOptions();
    if (sync_snapshots_->Get(&s, &seq)) {
        read_option.snapshot = s;
    }
    leveldb::Status status = db_->Get(read_option, key, &value);
    sync_snapshots_->Release(seq);
    if (!status.ok()) {
        LOG(DEBUG, "GetFromStore get fail %s %s",
            key.substr(8).c_str(), status.ToString().c_str());
        sync_snapshots_->Release(seq);
        return false;
    }
    if (!info->ParseFromString(value)) {
        LOG(WARNING, "GetFromStore parse fail %s", key.substr(8).c_str());
        sync_snapshots_->Release(seq);
        return false;
    }
    return true;
}

void NameSpace::SetupRoot() {
    root_path_.set_entry_id(kRootEntryid);
    root_path_.set_name("");
    root_path_.set_parent_entry_id(kRootEntryid);
    root_path_.set_type(01755);
    root_path_.set_ctime(static_cast<uint32_t>(version_/1000000));
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
    FileInfo file_info_for_ldb;
    file_info_for_ldb.CopyFrom(file_info);
    file_info_for_ldb.clear_cs_addrs();

    std::string file_key;
    EncodingStoreKey(file_info_for_ldb.parent_entry_id(), file_info_for_ldb.name(), &file_key);
    std::string infobuf_for_ldb, infobuf_for_sync;
    file_info_for_ldb.SerializeToString(&infobuf_for_ldb);
    file_info.SerializeToString(&infobuf_for_sync);

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
    for (int i=0; i < depth-1; ++i) {
        if (!LookUp(parent_id, paths[i], &file_info)) {
            file_info.set_type((1<<9)|0755);
            file_info.set_ctime(time(NULL));
            file_info.set_entry_id(common::atomic_add64(&last_entry_id_, 1) + 1);
            file_info.SerializeToString(&info_value);
            std::string key_str;
            EncodingStoreKey(parent_id, paths[i], &key_str);
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

    const std::string& fname = paths[depth-1];
    bool exist = LookUp(parent_id, fname, &file_info);
    if (exist) {
        if ((flags & O_TRUNC) == 0) {
            LOG(INFO, "CreateFile %s fail: already exist!", fname.c_str());
            return kNotOK;
        } else {
            for (int i = 0; i < file_info.blocks_size(); i++) {
                blocks_to_remove->push_back(file_info.blocks(i));
            }
        }
    }
    if (mode) {
        file_info.set_type(((1 << 10) - 1) & mode);
    } else {
        file_info.set_type(0755);
    }
    file_info.set_entry_id(common::atomic_add64(&last_entry_id_, 1) + 1);
    file_info.set_ctime(time(NULL));
    file_info.set_replicas(replica_num <= 0 ? FLAGS_default_replica_num : replica_num);
    //file_info.add_blocks();
    file_info.SerializeToString(&info_value);
    std::string file_key;
    EncodingStoreKey(parent_id, fname, &file_key);
    EncodeLog(log, kSyncWrite, file_key, info_value);
    return kOK;
}

StatusCode NameSpace::ListDirectory(const std::string& path,
                             google::protobuf::RepeatedPtrField<FileInfo>* outputs) {
    outputs->Clear();
    FileInfo info;
    if (!LookUp(path, &info)) {
        return kNotFound;
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
        return kNotFound;
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
            return kNotFound;
        }
        if (!IsDir(path_file.type())) {
            LOG(INFO, "Rename %s to %s fail: %s is not a directory",
                old_path.c_str(), new_path.c_str(), new_paths[i].c_str());
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
                return kNotOK;
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

    EncodeLog(log, kSyncWrite, new_key, value);
    EncodeLog(log, kSyncDelete, old_key, "");
    return kOK;
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
        ret_status = kNotFound;
    }
    return ret_status;
}

StatusCode NameSpace::DeleteDirectory(const std::string& path, bool recursive,
                               std::vector<FileInfo>* files_removed, NameServerLog* log) {
    files_removed->clear();
    FileInfo info;
    std::string store_key;
    if (!LookUp(path, &info)) {
        LOG(INFO, "Delete Directory, %s is not found.", path.c_str());
        return kNotFound;
    } else if (!IsDir(info.type())) {
        LOG(INFO, "Delete Directory, %s %d is not a dir.", path.c_str(), info.type());
        return kNotOK;
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
        return kNotOK;
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
        LOG(FATAL, "Namespace write to storage fail!");
        LOG(INFO, "Unlink dentry fail: %s\n", dir_info.name().c_str());
        ret_status = kUpdateError;
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

void NameSpace::TailLog(const std::string& logstr, int64_t seq) {
    NameServerLog log;
    if(!log.ParseFromString(logstr)) {
        LOG(FATAL, "Parse log fail: %s", common::DebugString(logstr).c_str());
    }
    if (seq != -1) {
        sync_snapshots_->Add(seq);
        LOG(INFO, "SyncSnapshot add %ld", seq);
    }
    leveldb::WriteBatch batch;
    for (int i = 0; i < log.entries_size(); i++) {
        const NsLogEntry& entry = log.entries(i);
        int type = entry.type();
        if (type == kSyncWrite) {
            batch.Put(entry.key(), entry.value());
        } else if (type == kSyncDelete) {
            batch.Delete(entry.key());
        }
    }
    leveldb::Status s = db_->Write(leveldb::WriteOptions(), &batch);
    if (!s.ok()) {
        LOG(FATAL, "TailLog failed");
    }
}

void NameSpace::CleanSnapshot(int64_t seq) {
    sync_snapshots_->Release(seq);
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
    next_block_id_ = block_id_upbound_ + 1;
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

int64_t NameSpace::GetNewBlockId(NameServerLog* log) {
    MutexLock lock(&mu_);
    if (next_block_id_ == block_id_upbound_) {
        UpdateBlockIdUpbound(log);
        return next_block_id_;
    } else {
        return next_block_id_++;
    }
}

} // namespace bfs
} // namespace baidu
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
