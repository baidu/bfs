// Copyright (c) 2014, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#ifndef  COMMON_LOCK_MUTEX_H_
#define  COMMON_LOCK_MUTEX_H_

#include <stdint.h>
#include <stdlib.h>
#include <pthread.h>
#include <sys/time.h>

namespace common {

// A Mutex represents an exclusive lock.
class Mutex {
public:
    Mutex()
        : owner_(0) {
        pthread_mutex_init(&mu_, NULL);
    }
    ~Mutex() {
        pthread_mutex_destroy(&mu_);
    }
    // Lock the mutex.
    // Will deadlock if the mutex is already locked by this thread.
    void Lock() {
        pthread_mutex_lock(&mu_);
        owner_ = pthread_self();
    } 
    // Unlock the mutex.
    void Unlock() {
        owner_ = 0;
        pthread_mutex_unlock(&mu_);
    }
    // Crash if this thread does not hold this mutex.
    void AssertHeld() {
         if(0 == pthread_equal(owner_, pthread_self())) {
            abort();
         }
    }
private:
    friend class CondVar;
    Mutex(const Mutex&);
    void operator=(const Mutex&);
    pthread_mutex_t mu_;
    pthread_t owner_;
};

// Mutex lock guard
class MutexLock {
public:
    explicit MutexLock(Mutex *mu) : mu_(mu) {
        mu_->Lock();
    }
    ~MutexLock() {
        mu_->Unlock();
    }
private:
    Mutex *const mu_;
    MutexLock(const MutexLock&);
    void operator=(const MutexLock&);
};

// Conditional variable
class CondVar {
public:
    explicit CondVar(Mutex* mu) : mu_(mu) {
        pthread_cond_init(&cond_, NULL);
    }
    ~CondVar() {
        pthread_cond_destroy(&cond_);
    }
    void Wait() {
        mu_->owner_ = 0;
        pthread_cond_wait(&cond_, &mu_->mu_);
        mu_->owner_ = pthread_self();
    }
    // Time wait in ms
    void TimeWait(int timeout) {
        timespec ts;
        struct timeval tv;
        gettimeofday(&tv, NULL);
        int64_t usec = tv.tv_usec + timeout * 1000LL;
        ts.tv_sec = tv.tv_sec + usec / 1000000;
        ts.tv_nsec = (usec % 1000000) * 1000;
        mu_->owner_ = 0;
        pthread_cond_timedwait(&cond_, &mu_->mu_, &ts);
        mu_->owner_ = pthread_self();
    }
    void Signal() {
        pthread_cond_signal(&cond_);
    }
    void Broadcast() {
        pthread_cond_broadcast(&cond_);
    }
private:
    CondVar(const CondVar&);
    void operator=(const CondVar&);
    Mutex* mu_;
    pthread_cond_t cond_;
};
}

using common::Mutex;
using common::MutexLock;
using common::CondVar;

#endif  // COMMON_LOCK_MUTEX_H_

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
