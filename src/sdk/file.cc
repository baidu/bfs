// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//

#include "fs_impl.h"
#include "file_impl.h"

#include <boost/shared_ptr.hpp>

namespace baidu {
namespace bfs {

File::File(FSImpl* fs, RpcClient* rpc_client,
           const std::string& name, int32_t flags, const WriteOptions& options) :
           impl_(new FileImpl(fs, rpc_client, name, flags, options)) {}

File::File(FSImpl* fs, RpcClient* rpc_client,
           const std::string& name, int32_t flags, const ReadOptions& options) :
           impl_(new FileImpl(fs, rpc_client, name, flags, options)) {}

File::File(FileImpl* file_impl) : impl_(file_impl) {}

int32_t File::Pread(char* buf, int32_t read_size, int64_t offset, bool reada) {
    return impl_->Pread(buf, read_size, offset, reada);
}

int64_t File::Seek(int64_t offset, int32_t whence) {
    return impl_->Seek(offset, whence);
}

int32_t File::Read(char* buf, int32_t read_size) {
    return impl_->Read(buf, read_size);
}

int32_t File::Write(const char* buf, int32_t write_size) {
    return impl_->Write(buf, write_size);
}

int32_t File::Flush() {
    return impl_->Flush();
}

int32_t File::Sync() {
    return impl_->Sync();
}

int32_t File::Close() {
    return impl_->Close();
}

} // namespace bfs
} // namespace baidu
