// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//

#include <vector>
#include <string>
#include <iostream>
#include <functional>
#include <assert.h>

#include <common/string_util.h>
#include <common/thread_pool.h>
#include <gflags/gflags.h>

#include "mark.h"

DECLARE_string(flagfile);
DECLARE_string(nameserver_nodes);

DEFINE_string(mode, "put", "[put | read]");
DEFINE_int64(count, 0, "put/read/delete file count");
DEFINE_int32(thread, 5, "thread num");
DEFINE_int32(seed, 301, "random seed");
DEFINE_int64(file_size, 1024, "file size in KB");
DEFINE_string(folder, "test", "write data to which folder");
DEFINE_bool(break_on_failure, true, "exit when error occurs");

namespace baidu {
namespace bfs {

// borrowed from LevelDB
class Random {
private:
    uint32_t seed_;
public:
    explicit Random(uint32_t s) : seed_(s & 0x7fffffffu) { }
    uint32_t Next() {
        static const uint32_t M = 2147483647L;
        static const uint64_t A = 16807;
        uint64_t product = seed_ * A;
        seed_ = static_cast<uint32_t>((product >> 31) + (product & M));
        if (seed_ > M) {
            seed_ -= M;
        }
        return seed_;
    }
    uint32_t Uniform(int n) { return Next() % n; }
};

Mark::Mark() : fs_(NULL), file_size_(FLAGS_file_size << 10), exit_(false) {
    if (!FS::OpenFileSystem(FLAGS_nameserver_nodes.c_str(), &fs_, FSOptions())) {
        std::cerr << "Open filesytem failed " << FLAGS_nameserver_nodes << std::endl;
        exit(EXIT_FAILURE);
    }

    thread_pool_ = new common::ThreadPool(FLAGS_thread + 1);
    rand_ = new Random*[FLAGS_thread];
    for (int i = 0; i < FLAGS_thread; i++) {
        rand_[i] = new Random(FLAGS_seed + i);
    }
}

bool Mark::FinishPut(File* file, int thread_id) {
    if (file) {
        if (OK != file->Close()) {
            delete file;
            rand_[thread_id]->Next();
            return false;
        }
        delete file;
    }
    rand_[thread_id]->Next();
    return true;
}

void Mark::Put(const std::string& filename, const std::string& base, int thread_id) {
    File* file;
    if (OK != fs_->OpenFile(filename.c_str(), O_WRONLY | O_TRUNC, 664, &file, WriteOptions())) {
        if (FLAGS_break_on_failure) {
            std::cerr << "OpenFile failed " << filename << std::endl;
            exit(EXIT_FAILURE);
        } else {
            FinishPut(file, thread_id);
            std::cerr << "[Failed] " << filename << std::endl;
            return;
        }
    }
    int64_t len = 0;
    int64_t base_size = (1 << 20) / 2;
    while (len < file_size_) {
        uint64_t w = base_size + rand_[thread_id]->Uniform(base_size);

        uint32_t write_len = file->Write(base.c_str(), w);
        if (write_len != w) {
            if (FLAGS_break_on_failure) {
                std::cerr << "Write length does not match write_len = "
                        << write_len << " should be " << w << std::endl;
                exit(EXIT_FAILURE);
            } else {
                FinishPut(file, thread_id);
                std::cerr << "[Failed] " << filename << std::endl;
                return;
            }
        }
        len += write_len;
    }
    if (!FinishPut(file, thread_id)) {
        if (FLAGS_break_on_failure) {
            std::cerr << "Close file failed " << filename << std::endl;
            exit(EXIT_FAILURE);
        } else {
            std::cerr << "[Failed] " << filename << std::endl;
            return;
        }
    }
    put_counter_.Inc();
    all_counter_.Inc();
}

bool Mark::FinishRead(File* file) {
    if (file) {
        if (OK != file->Close()) {
            delete file;
            return false;
        }
        delete file;
    }
    return true;
}

void Mark::Read(const std::string& filename, const std::string& base, int thread_id) {
    File* file;
    if (OK != fs_->OpenFile(filename.c_str(), O_RDONLY, &file, ReadOptions())) {
        if (FLAGS_break_on_failure) {
            std::cerr << "Open file failed " << filename << std::endl;
            exit(EXIT_FAILURE);
        } else {
            FinishRead(file);
            std::cerr << "[Failed] " << filename << std::endl;
            return;
        }
    }
    int64_t buf_size = 1 << 20;
    int64_t base_size = buf_size / 2;
    char buf[buf_size];
    int64_t bytes = 0;
    int32_t len = 0;
    while (1) {
        uint64_t r = base_size + rand_[thread_id]->Uniform(base_size);
        len = file->Read(buf, r);
        if (len < 0) {
            if (FLAGS_break_on_failure) {
                std::cerr << "Read length error" << std::endl;
                exit(EXIT_FAILURE);
            } else {
                FinishRead(file);
                std::cerr << "[Failed] " << filename << std::endl;
                return;
            }
        }
        if (len == 0) {
            break;
        }
        if (base.substr(0, len) != std::string(buf, len)) {
            if (FLAGS_break_on_failure) {
                std::cerr << "Read varify failed " << filename << " : bytes = " << bytes
                        << " len = " << len << " r = " << r << std::endl;
                exit(EXIT_FAILURE);
            } else {
                FinishRead(file);
                std::cerr << "[Failed] " << filename << std::endl;
                return;
            }
        }
        bytes += len;
    }
    BfsFileInfo info;
    fs_->Stat(filename.c_str(), &info);
    if (bytes != info.size) {
        if (FLAGS_break_on_failure) {
            std::cerr << "File size mismatch " << filename << " size = " << bytes
                    << " should be " << info.size << std::endl;
            exit(EXIT_FAILURE);
        } else {
            FinishRead(file);
            std::cerr << "[Failed] " << filename << std::endl;
            return;
        }
    }
    if (!FinishRead(file)) {
        if (FLAGS_break_on_failure) {
            std::cerr << "Close file failed " << filename << std::endl;
            exit(EXIT_FAILURE);
        } else {
            std::cerr << "[Failed] " << filename << std::endl;
            return;
        }
    }
    read_counter_.Inc();
    all_counter_.Inc();
}

void Mark::Delete(const std::string& filename) {
    if(OK != fs_->DeleteFile(filename.c_str())) {
        assert(0);
    }
    del_counter_.Dec();
}

void Mark::PutWrapper(int thread_id) {
    std::string prefix = common::NumToString(thread_id);
    int name_id = 0;
    int64_t count = 0;
    std::string base;
    RandomString(&base, 1<<20, thread_id);
    while (FLAGS_count == 0 || count != FLAGS_count) {
        std::string filename = "/" + FLAGS_folder + "/" + prefix + "/" + common::NumToString(name_id);
        Put(filename, base, thread_id);
        ++name_id;
        ++count;
    }
    exit_ = true;
}

void Mark::ReadWrapper(int thread_id) {
    std::string prefix = common::NumToString(thread_id);
    int name_id = 0;
    int64_t count = 0;
    std::string base;
    RandomString(&base, 1<<20, thread_id);
    while (FLAGS_count == 0 || count != FLAGS_count) {
        std::string filename = "/" + FLAGS_folder + "/" + prefix + "/" + common::NumToString(name_id);
        Read(filename, base, thread_id);
        ++name_id;
        ++count;
    }
    exit_ = true;
}

void Mark::PrintStat() {
    std::cout << "Put\t" << put_counter_.Get() << "\tDel\t" << del_counter_.Get()
              << "\tRead\t" << read_counter_.Get() << "\tAll\t" << all_counter_.Get() << std::endl;
    put_counter_.Set(0);
    del_counter_.Set(0);
    read_counter_.Set(0);
    thread_pool_->DelayTask(1000, std::bind(&Mark::PrintStat, this));
}

void Mark::Run() {
    PrintStat();
    if (FLAGS_mode == "put") {
        fs_->CreateDirectory(("/" + FLAGS_folder).c_str());
        for (int i = 0; i < FLAGS_thread; ++i) {
            std::string prefix = common::NumToString(i);
            fs_->CreateDirectory(("/" + FLAGS_folder + "/" + prefix).c_str());
        }
        for (int i = 0; i < FLAGS_thread; ++i) {
            thread_pool_->AddTask(std::bind(&Mark::PutWrapper, this, i));
        }
    } else if (FLAGS_mode == "read") {
        for (int i = 0; i < FLAGS_thread; ++i) {
            thread_pool_->AddTask(std::bind(&Mark::ReadWrapper, this, i));
        }
    }
    while (!exit_) {
        sleep(1);
    }
    thread_pool_->Stop(true);
    if (FLAGS_count != 0) {
        std::cout << "Total " << FLAGS_mode << " " << FLAGS_count * FLAGS_thread << std::endl;
    }
}

void Mark::RandomString(std::string* out, int size, int rand_index) {
    out->resize(size);
    for (int i = 0; i < size; i++) {
        (*out)[i] = static_cast<char>(' ' + rand_[rand_index]->Next());
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
