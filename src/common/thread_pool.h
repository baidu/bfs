// Copyright (c) 2014, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#ifndef  COMMON_THREAD_POOL_H_
#define  COMMON_THREAD_POOL_H_

#include <pthread.h>
#include <algorithm>
#include <queue>
#include <vector>

#include "mutex.h"

namespace common {

typedef void* (*ThreadFunc)(void* arg);

static pthread_t StartThread(ThreadFunc proc, void* arg) {
    pthread_t tid;
    pthread_create(&tid, NULL, proc, arg);
    return tid;
}

class ThreadPool {
public:
    ThreadPool(int num) 
      : _taskq_cond(&_taskq_mu), _stopped(false) {
        for (int i=0; i<num; i++) {
            pthread_t tid;
            if (0 == pthread_create(&tid, NULL, BGWrapper, NULL)) {
                _tids.push_back(tid);
            }
        }
    }
    virtual ~ThreadPool() {
        _stopped = true;
        for (size_t i=0; i<_tids.size(); i++) {
            pthread_join(_tids[i], NULL);
        }
    }
    virtual void Schedule(ThreadFunc func, void* arg) {
        MutexLock lock(&_taskq_mu);
        _taskq.push_back(std::make_pair(func, arg));
    }
private:
    static void* BGWrapper(void* arg) {
        (reinterpret_cast<ThreadPool*>(arg))->BGWork();
        return NULL;
    }
    void BGWork() {
        while (!_stopped) {
            ThreadFunc func = NULL;
            void* arg = NULL;
            _taskq_mu.Lock();
            if (_taskq.empty()) {
                _taskq_cond.Wait();
                _taskq_mu.Unlock();
                continue;
            }
            func = _taskq.front().first;
            arg = _taskq.front().second;
            _taskq.pop_front();
            _taskq_mu.Unlock();
            func(arg);
        }
    }
private:
    std::vector<pthread_t> _tids;
    std::deque< std::pair<ThreadFunc, void*> > _taskq;
    Mutex   _taskq_mu;
    CondVar _taskq_cond;
    bool _stopped;
};

} // namespce common

#endif  //COMMON_THREAD_POOL_H_

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
