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
#include "timer.h"

namespace common {

// #define MUTEX_DEBUG

// A Mutex represents an exclusive lock.
class Mutex {
public:
    Mutex()
        : owner_(0), msg_(NULL), msg_threshold_(0), lock_time_(0) {
        pthread_mutex_init(&mu_, NULL);
    }
    ~Mutex() {
        pthread_mutex_destroy(&mu_);
    }
    // Lock the mutex.
    // Will deadlock if the mutex is already locked by this thread.
    void Lock(const char* msg = NULL, int64_t msg_threshold = 100) {
        #ifdef MUTEX_DEBUG
        int64_t s = 0;
        if (msg) {
            s = timer::get_micros();
        }
        #endif
        pthread_mutex_lock(&mu_);
        AfterLock(msg, msg_threshold);
        #ifdef MUTEX_DEBUG_
        if (msg && lock_time_ - s > msg_threshold) {
            printf("%s wait lock %.3f ms\n", msg, (lock_time_ -s) / 1000.0);
        }
        #endif
    } 
    // Unlock the mutex.
    void Unlock() {
        BeforeUnlock();
        pthread_mutex_unlock(&mu_);
    }
    // Crash if this thread does not hold this mutex.
    void AssertHeld() {
         if(0 == pthread_equal(owner_, pthread_self())) {
            abort();
         }
    }
private:
    void AfterLock(const char* msg, int64_t msg_threshold) {
        #ifdef MUTEX_DEBUG
        msg_ = msg;
        msg_threshold_ = msg_threshold;
        if (msg_) {
            lock_time_ = timer::get_micros();
        }
        #endif
        owner_ = pthread_self();
    }
    void BeforeUnlock() {
        #ifdef MUTEX_DEBUG
        if (msg_ && timer::get_micros() - lock_time_ > msg_threshold_) {
            printf("%s locked %.3f ms\n", msg_, (timer::get_micros() - lock_time_) / 1000.0);
        }
        msg_ = NULL;
        #endif
        owner_ = 0;
    }
private:
    friend class CondVar;
    Mutex(const Mutex&);
    void operator=(const Mutex&);
    pthread_mutex_t mu_;
    pthread_t owner_;
    const char* msg_;
    int64_t msg_threshold_;
    int64_t lock_time_;
};

// Mutex lock guard
class MutexLock {
public:
    explicit MutexLock(Mutex *mu, const char* msg = NULL) : mu_(mu) {
        mu_->Lock(msg);
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
    void Wait(const char* msg = NULL) {
        int64_t msg_threshold = mu_->msg_threshold_;
        mu_->BeforeUnlock();
        pthread_cond_wait(&cond_, &mu_->mu_);
        mu_->AfterLock(msg, msg_threshold);
    }
    // Time wait in m
    bool TimeWait(int timeout, const char* msg = NULL) {
        timespec ts;
        struct timeval tv;
        gettimeofday(&tv, NULL);
        int64_t usec = tv.tv_usec + timeout * 1000LL;
        ts.tv_sec = tv.tv_sec + usec / 1000000;
        ts.tv_nsec = (usec % 1000000) * 1000;
        int64_t msg_threshold = mu_->msg_threshold_;
        mu_->BeforeUnlock();
        int ret = pthread_cond_timedwait(&cond_, &mu_->mu_, &ts);
        mu_->AfterLock(msg, msg_threshold);
        return (ret == 0);
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
