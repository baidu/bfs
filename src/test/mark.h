// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//

#include <common/counter.h>

#include "sdk/bfs.h"

namespace baidu {
namespace bfs {

class Mark {
public:
    Mark();
    void Put(const std::string& filename, int len);
    void Delete(const std::string& filename);
    void PutWrapper(int thread_id);
    void PrintStat();
    void Run();
private:
    FS* fs_;
    common::Counter put_counter_;
    common::Counter del_counter_;
    common::ThreadPool* thread_pool_;
};

} // namespace bfs
} // namespace baidu
