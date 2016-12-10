// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//

#include <common/counter.h>

#include "sdk/bfs.h"

namespace baidu {
namespace bfs {

class Random;

class Mark {
public:
    Mark();
    void Put(const std::string& filename, const std::string& base, int thread_id);
    bool FinishPut(File* file, int thread_id);
    void Read(const std::string& filename, const std::string& base, int thread_id);
    bool FinishRead(File* file);
    void Delete(const std::string& filename);
    void PutWrapper(int thread_id);
    void ReadWrapper(int thread_id);
    void PrintStat();
    void Run();
private:
    void RandomString(std::string* out, int size, int rand_index);
private:
    FS* fs_;
    common::Counter put_counter_;
    common::Counter del_counter_;
    common::Counter read_counter_;
    common::Counter all_counter_;
    common::ThreadPool* thread_pool_;
    Random** rand_;
    int64_t file_size_;
    volatile bool exit_;
    volatile bool has_error_;
};

} // namespace bfs
} // namespace baidu
