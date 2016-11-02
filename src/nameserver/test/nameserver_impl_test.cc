// Copyright (c) 2014, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//

#include "proto/nameserver.pb.h"
#include "proto/file.pb.h"
#include "nameserver/nameserver_impl.h"

#include <iostream>
#include <string>
#include <functional>

#include <sofa/pbrpc/pbrpc.h>
#include <gtest/gtest.h>
#include <gflags/gflags.h>
#include <common/counter.h>
#include <common/string_util.h>
#include <common/timer.h>
#include <common/thread_pool.h>

DECLARE_string(bfs_log);
DECLARE_int32(nameserver_work_thread_num);

namespace baidu {
namespace bfs {

class NameServerImplTest : public ::testing::Test {
public:
    NameServerImplTest() {}
protected:
};

common::Counter create_counter;
common::ThreadPool thread_pool(5);
bool stop = false;

void callback(CreateFileRequest* request, CreateFileResponse* response) {
    delete request;
    delete response;
    create_counter.Inc();
}

void CreateWorder(NameServerImpl* nameserver, sofa::pbrpc::RpcController* cntl, const std::string& prefix) {
    int i = 0;
    while(!stop) {
        CreateFileRequest* request = new CreateFileRequest;
        CreateFileResponse* response = new CreateFileResponse;
        request->set_file_name(prefix + "/" + common::NumToString(i));
        request->set_sequence_id(0);
        ::google::protobuf::Closure* done = sofa::pbrpc::NewClosure(callback, request, response);
        nameserver->CreateFile(cntl, request, response, done);
        ++i;
    }
}

TEST_F(NameServerImplTest, CreateFile) {
    system("rm -rf ./db");
    FLAGS_bfs_log = "./bfs.log";
    FLAGS_nameserver_work_thread_num = 10;
    NameServerImpl nameserver(NULL);
    sofa::pbrpc::RpcController controller;

    for (int i = 0; i < 10; ++i) {
        thread_pool.AddTask(std::bind(&CreateWorder, &nameserver, &controller, common::NumToString(i)));
    }
    uint64_t count = create_counter.Get();
    uint64_t start = common::timer::get_micros();
    sleep(10);
    stop = true;
    uint64_t interval = common::timer::get_micros() - start;
    std::cerr << (create_counter.Get() - count) * 1000000.0 / interval << std::endl;
}

TEST_F(NameServerImplTest, NameServerImpl) {
    FileInfo info;
    uint64_t start = common::timer::get_micros();
    std::string value;
    FileInfo decode;
    for (int i = 0; i < 100000; ++i) {
        info.set_entry_id(0);
        info.set_version(0);
        info.set_type(0);
        info.set_ctime(0);
        info.set_name("helloasdfasdfasdf");
        info.set_size(0);
        info.set_replicas(3);
        info.set_parent_entry_id(2);
        info.set_owner(0);
        info.SerializeToString(&value);
        //decode.ParseFromString(value);
    }
    uint64_t interval = common::timer::get_micros() - start;
    std::cerr << 100000.0 * 1000000.0 / interval << std::endl;
}

} // namespace baidu
} // namespace bfs

int main(int argc, char** argv) {
    ::google::ParseCommandLineFlags(&argc, &argv, false);
    ::testing::InitGoogleTest(&argc, argv);
    std::cout << "hello\n" << std::endl;
    return RUN_ALL_TESTS();
}
