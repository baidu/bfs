// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//

#ifndef  BFS_SDK_FILE_IMPL_WRAPPER_H_
#define  BFS_SDK_FILE_IMPL_WRAPPER_H_

#include "bfs.h"

#include <boost/shared_ptr.hpp>

namespace baidu {
namespace bfs {

class FileImpl;
class FSImpl;
class RpcClient;

class FileImplWrapper : public File {
public:
    FileImplWrapper(FSImpl* fs, RpcClient* rpc_client,
            const std::string& name, int32_t flags, const WriteOptions& options);
    FileImplWrapper(FSImpl* fs, RpcClient* rpc_client,
            const std::string& name, int32_t flags, const ReadOptions& options);
    FileImplWrapper(FileImpl* file_impl);
    virtual ~FileImplWrapper() {}
    virtual int32_t Pread(char* buf, int32_t read_size, int64_t offset, bool reada = false);
    virtual int64_t Seek(int64_t offset, int32_t whence);
    virtual int32_t Read(char* buf, int32_t read_size);
    virtual int32_t Write(const char* buf, int32_t write_size);
    virtual int32_t Flush();
    virtual int32_t Sync();
    virtual int32_t Close();
private:
    boost::shared_ptr<FileImpl> impl_;
    // No copying allowed
    FileImplWrapper(const FileImplWrapper&);
    void operator=(const FileImplWrapper&);
};

} // namespace bfs
} // namespace baidu

#endif  // FILE_IMPL_WRAPPER_H_
