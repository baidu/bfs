// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//

#include <gflags/gflags.h>
#include <vector>
#include <string>
#include <iostream>
#include <common/string_util.h>
#include <common/thread_pool.h>
#include <boost/bind.hpp>

#include "mark.h"

DECLARE_string(flagfile);
DECLARE_string(nameserver_nodes);

DEFINE_int32(put, 5, "put thread num");
DEFINE_int32(thread, 30, "thread num");

namespace baidu {
namespace bfs {

Mark::Mark() : fs_(NULL) {
    if (!FS::OpenFileSystem(FLAGS_nameserver_nodes.c_str(), &fs_)) {
        fprintf(stderr, "Open filesytem %s fail\n", FLAGS_nameserver_nodes.c_str());
        assert(0);
    }

    thread_pool_ = new common::ThreadPool(FLAGS_thread);
}

void Mark::Put(const std::string& filename, int len) {
    File* file;
    if (!fs_->OpenFile(filename.c_str(), O_WRONLY | O_TRUNC, 664, -1, &file)) {
        fprintf(stderr, "Can't Open bfs file %s\n", filename.c_str());
        assert(0);
    }
    std::string buf(len, 'x');
    file->Write(buf.c_str(), len);
    if (!file->Close()) {
        fprintf(stderr, "close fail: %s\n", filename.c_str());
        assert(0);
    }
    delete file;
    put_counter_.Inc();
}

void Mark::Delete(const std::string& filename) {
    if(!fs_->DeleteFile(filename.c_str())) {
        assert(0);
    }
    del_counter_.Dec();
}

void Mark::PutWrapper(int thread_id) {
    std::string prefix = common::NumToString(thread_id);
    int name_id = 0;
    while (true) {
        std::string filename = "/" + prefix + "/" + common::NumToString(name_id);
        Put(filename, 3);
        ++name_id;
    }
}
void Mark::PrintStat() {
    std::cout << "Put\t" << put_counter_.Get() << " \t" << "Del\t" << del_counter_.Get() << std::endl;
    put_counter_.Set(0);
    del_counter_.Set(0);
    thread_pool_->DelayTask(1000, boost::bind(&Mark::PrintStat, this));
}

void Mark::Run() {
    PrintStat();
    for (int i = 0; i < FLAGS_put; ++i) {
        thread_pool_->AddTask(boost::bind(&Mark::PutWrapper, this, i));
    }
    while (true) {
        sleep(1);
    }
}

} // namespace bfs
} // namespace baidu

int main(int argc, char* argv[]) {
    FLAGS_flagfile = "bfs.flag";
    ::google::ParseCommandLineFlags(&argc, &argv, false);
    baidu::bfs::Mark mark;
    mark.Run();
    return 0;
}
