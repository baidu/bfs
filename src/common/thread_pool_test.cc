// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#include <assert.h>
#include <stdio.h>
#include <vector>
#include <map>
#include <boost/bind.hpp>
#include "atomic.h"
#include "thread_pool.h"


volatile int finished_num = 0;
void Print(int t) {
    printf("[%d] Task %d run\n", finished_num, t);
    common::atomic_inc(&finished_num);
}

int main() {
    ThreadPool tp(10);
    bool r = tp.Start();
    assert(r);

    std::vector<std::pair<int64_t, int> > scheduled_tasks;
    for (int i=0; i < 15; i++) {
        int tid = tp.DelayTask(boost::bind(Print, i), (15 - i) * 100);
        scheduled_tasks.push_back(std::make_pair(tid, i));
        int cancel_tid = rand()%scheduled_tasks.size();
        bool ret = tp.CancelTask(scheduled_tasks[cancel_tid].first);
        if (ret) {
            printf("Task %d is canceld\n", scheduled_tasks[cancel_tid].second);
            common::atomic_inc(&finished_num);
        }
    }
    while (finished_num < 15) {
        usleep(100);
    }
    tp.Stop(true);
    return 0;
}
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
