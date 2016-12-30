// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//

#ifndef  BFS_UTILS_META_CONVERTER_H_
#define  BFS_UTILS_META_CONVERTER_H_

namespace baidu {
namespace bfs {

void ChunkserverMetaV0(const std::map<std::string, leveldb::DB*>& meta_dbs);
void SetChunkserverMetaVersion(const std::map<std::string, leveldb::DB*>& meta_dbs);
void CleanUp(const std::map<std::string, leveldb::DB*>& meta_dbs);
void CheckChunkserverMeta(const std::vector<std::string>& store_path_list);

} // namespace bfs
} // namespace baidu

#endif  //BFS_UTILS_META_CONVERTER_H_
