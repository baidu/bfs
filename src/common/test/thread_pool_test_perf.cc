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
#define MUTEX_DEBUG
#include "thread_pool.h"
#include "timer.h"


volatile int finished_num = 0;
volatile int delay_num = 0;
ThreadPool thread_pool(50);

int64_t start_time = 0;
void Print(bool is_delay) {
    int num = 0;
    if (is_delay) {
        num = common::atomic_add(&delay_num, 1) + 1;
        if (num % 10000 == 0) {
            printf("%d delay tasks finish\n", num);
        }
    } else {
        num = common::atomic_add(&finished_num, 1) + 1;
        if (num % 10000 == 0) {
            printf("%d tasks finish, %d task pending, %.2f/s\n",
                   num, thread_pool.PendingNum(),
                   num / ((common::timer::get_micros() - start_time)/1000000.0));
        }
    }
}


void ThrowTasks(ThreadPool* thread_pool, int num) {
    for (int i = 0; i < num; i++) {
        thread_pool->AddTask(boost::bind(Print, false));
    }
}
void ThrowDelayTasks(ThreadPool* thread_pool, int num) {
    if (delay_num < num) {
        thread_pool->DelayTask(10, boost::bind(Print, true));
        thread_pool->AddPriorityTask(boost::bind(ThrowDelayTasks, thread_pool, 1000000));
    }
}
int main() {
    ThreadPool workers(8);
    workers.Start();

    start_time = common::timer::get_micros();
    for (int i=0; i < 6; i++) {
        workers.AddTask(boost::bind(ThrowTasks, &thread_pool, 10000000));
    }
    workers.AddTask(boost::bind(ThrowDelayTasks, &thread_pool, 1000000));
    while (true) {
        sleep(1);
    }
    return 0;
}
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
