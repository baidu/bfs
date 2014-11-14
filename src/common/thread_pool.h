// Copyright (c) 2014, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#ifndef  COMMON_THREAD_POOL_H_
#define  COMMON_THREAD_POOL_H_

#include <deque>
#include <vector>
#include <boost/function.hpp>
#include "mutex.h"

namespace common {

// An unscalable thread pool impliment.
class ThreadPool {
public:
    ThreadPool(int thread_num)
      : threads_num_(thread_num),
        pending_num_(0),
        cond_(&mu_), stop_(false) {
    }
    ~ThreadPool() {
        Stop(false);
    }
    // Start a thread_num threads pool.
    bool Start() {
        stop_ = false;
        for (int i = 0; i < threads_num_; i++) {
            pthread_t tid;
            pthread_create(&tid, NULL, ThreadWrapper, this);
            tids_.push_back(tid);
        }
        return true;
    }

    // Stop the thread pool.
    // Wait for all pending task to complete if wait is true.
    bool Stop(bool wait) {
        if (wait) {
            while(pending_num_ > 0) {
                usleep(10000);
            }
        }

        {
            MutexLock lock(&mu_);
            stop_ = true;
            cond_.Broadcast();
        }
        for (uint32_t i = 0; i < tids_.size(); i++) {
            pthread_join(tids_[i], NULL);
        }
        tids_.clear();
        return true;
    }

    // Task definition.
    typedef boost::function<void ()> Task;

    // Add a task to the thread pool.
    void AddTask(const Task& task) {
        MutexLock lock(&mu_);
        queue_.push_back(task);
        ++pending_num_;
        cond_.Signal();
    }
    void AddPriorityTask(const Task& task) {
        MutexLock lock(&mu_);
        queue_.push_front(task);
        ++pending_num_;
        cond_.Signal();
    }
private:
    ThreadPool(const ThreadPool&);
    void operator=(const ThreadPool&);
    
    static void* ThreadWrapper(void* arg) {
        reinterpret_cast<ThreadPool*>(arg)->ThreadProc();
        return NULL;
    }
    void ThreadProc() {
        while(1) {
            Task task;
            {
                MutexLock lock(&mu_);
                while (queue_.empty() && !stop_) {
                    cond_.Wait();
                }
                if (stop_) {
                    break;
                }
                task = queue_.front();
                queue_.pop_front();
                --pending_num_;
            }
            task();
        }
    }
private:
    int32_t threads_num_;
    std::deque<Task> queue_;
    volatile uint64_t pending_num_;
    Mutex mu_;
    CondVar cond_;
    bool stop_;
    std::vector<pthread_t> tids_;
};

} // namespace common

using common::ThreadPool;

#endif  //COMMON_THREAD_POOL_H_

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
