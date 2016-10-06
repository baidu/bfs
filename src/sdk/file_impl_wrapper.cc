// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//

#include "file_impl_wrapper.h"
#include "file_impl.h"

namespace baidu {
namespace bfs {

FileImplWrapper::FileImplWrapper(FSImpl* fs, RpcClient* rpc_client,
           const std::string& name, int32_t flags, const WriteOptions& options) :
           impl_(new FileImpl(fs, rpc_client, name, flags, options)) {}

FileImplWrapper::FileImplWrapper(FSImpl* fs, RpcClient* rpc_client,
           const std::string& name, int32_t flags, const ReadOptions& options) :
           impl_(new FileImpl(fs, rpc_client, name, flags, options)) {}

FileImplWrapper::FileImplWrapper(FileImpl* file_impl) : impl_(file_impl) {}

int32_t FileImplWrapper::Pread(char* buf, int32_t read_size, int64_t offset, bool reada) {
    return impl_->Pread(buf, read_size, offset, reada);
}

int64_t FileImplWrapper::Seek(int64_t offset, int32_t whence) {
    return impl_->Seek(offset, whence);
}

int32_t FileImplWrapper::Read(char* buf, int32_t read_size) {
    return impl_->Read(buf, read_size);
}

int32_t FileImplWrapper::Write(const char* buf, int32_t write_size) {
    return impl_->Write(buf, write_size);
}

int32_t FileImplWrapper::Flush() {
    return impl_->Flush();
}

int32_t FileImplWrapper::Sync() {
    return impl_->Sync();
}

int32_t FileImplWrapper::Close() {
    return impl_->Close();
}

} // namespace bfs
} // namespace baidu
