// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//

#ifndef  BFS_UTILS_META_CONVERTER_H_
#define  BFS_UTILS_META_CONVERTER_H_

namespace baidu {
namespace bfs {

// Check wether the chunkserver meta is in valid format.
// A meta store with an oloder version will be convert into current
// meta version automatically.
void CheckChunkserverMeta(const std::vector<std::string>& store_path_list);
// Convert meta from version 0 to version 1.
void ChunkserverMetaV02V1(const std::map<std::string, leveldb::DB*>& meta_dbs);
// Set current version for all meta store.
void SetChunkserverMetaVersion(const std::map<std::string, leveldb::DB*>& meta_dbs);
// Close all meta store.
void CloseMetaStore(const std::map<std::string, leveldb::DB*>& meta_dbs);

} // namespace bfs
} // namespace baidu

#endif  //BFS_UTILS_META_CONVERTER_H_
