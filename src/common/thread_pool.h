// Copyright (c) 2014, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#ifndef  COMMON_THREAD_POOL_H_
#define  COMMON_THREAD_POOL_H_

#include <deque>
#include <map>
#include <queue>
#include <vector>
#include <boost/function.hpp>
#include "mutex.h"
#include "timer.h"

namespace common {

// An unscalable thread pool implimention.
class ThreadPool {
public:
    ThreadPool(int thread_num = 10)
      : threads_num_(thread_num),
        pending_num_(0),
        work_cv_(&mutex_), stop_(false),
        last_task_id_(0),
        running_task_id_(0) {
        Start();
    }
    ~ThreadPool() {
        Stop(false);
    }
    // Start a thread_num threads pool.
    bool Start() {
        MutexLock lock(&mutex_);
        if (tids_.size()) {
            return false;
        }
        stop_ = false;
        for (int i = 0; i < threads_num_; i++) {
            pthread_t tid;
            int ret = pthread_create(&tid, NULL, ThreadWrapper, this);
            if (ret) {
                abort();
            }
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
            MutexLock lock(&mutex_);
            stop_ = true;
            work_cv_.Broadcast();
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
        MutexLock lock(&mutex_, "AddTask");
        queue_.push_back(task);
        ++pending_num_;
        work_cv_.Signal();
    }
    void AddPriorityTask(const Task& task) {
        MutexLock lock(&mutex_);
        queue_.push_front(task);
        ++pending_num_;
        work_cv_.Signal();
    }
    int64_t DelayTask(int64_t delay, const Task& task) {
        MutexLock lock(&mutex_);
        int64_t now_time = timer::get_micros() / 1000;
        int64_t exe_time = now_time + delay;
        BGItem bg_item = {++last_task_id_, exe_time, task, false};
        time_queue_.push(bg_item);
        latest_[bg_item.id] = bg_item;
        work_cv_.Signal();
        return bg_item.id;
    }
    /// Cancel a delayed task ,if running, wait
    bool CancelTask(int64_t task_id) {
        if (task_id == 0) {
            return false;
        }
        while (1) {
            {
                MutexLock lock(&mutex_);
                if (running_task_id_ != task_id) {
                    BGMap::iterator it = latest_.find(task_id);
                    if (it == latest_.end()) {
                        return false;
                    }
                    latest_.erase(it);
                    return true;
                }
            }
            timespec ts = {0, 100000};
            nanosleep(&ts, &ts);
        }
    }
    int64_t PendingNum() const {
        return pending_num_;
    }
private:
    ThreadPool(const ThreadPool&);
    void operator=(const ThreadPool&);
    
    static void* ThreadWrapper(void* arg) {
        reinterpret_cast<ThreadPool*>(arg)->ThreadProc();
        return NULL;
    }
    void ThreadProc() {
        while (true) {
            Task task;
            MutexLock lock(&mutex_, "ThreadProc");
            while (time_queue_.empty() && queue_.empty() && !stop_) {
                work_cv_.Wait("ThreadProcWait");
            }
            if (stop_) {
                break;
            }
            // Timer task
            if (!time_queue_.empty()) {
                int64_t now_time = timer::get_micros() / 1000;
                BGItem bg_item = time_queue_.top();
                if (now_time >= bg_item.exe_time) {
                    time_queue_.pop();
                    BGMap::iterator it = latest_.find(bg_item.id);
                    if (it!= latest_.end() && it->second.exe_time == bg_item.exe_time) {
                        task = bg_item.callback;
                        latest_.erase(it);
                        running_task_id_ = bg_item.id;
                        mutex_.Unlock();
                        task();
                        mutex_.Lock("ThreadProcRelock");
                        running_task_id_ = 0;
                    }
                    continue;
                } else if (queue_.empty() && !stop_) {
                    work_cv_.TimeWait(bg_item.exe_time - now_time, "ThreadProcTimeWait");
                    continue;
                }
            }
            // Normal task;
            if (!queue_.empty()) {
                task = queue_.front();
                queue_.pop_front();
                --pending_num_;
                mutex_.Unlock();
                task();
                mutex_.Lock("ThreadProcRelock2");
            }
        }
    }
private:
    struct BGItem {
        int64_t id;
        int64_t exe_time;
        Task callback;
        bool canceled;
        bool operator<(const BGItem& item) const {
            if (exe_time != item.exe_time) {
                return exe_time > item.exe_time;
            } else {
                return id > item.id;
            }
        }
    };
    typedef std::priority_queue<BGItem> BGQueue;
    typedef std::map<int64_t, BGItem> BGMap;

    int32_t threads_num_;
    std::deque<Task> queue_;
    volatile int pending_num_;
    Mutex mutex_;
    CondVar work_cv_;
    bool stop_;
    std::vector<pthread_t> tids_;

    BGQueue time_queue_;
    BGMap latest_;
    int64_t last_task_id_;
    int64_t running_task_id_;
};

} // namespace common

using common::ThreadPool;

#endif  //COMMON_THREAD_POOL_H_

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
