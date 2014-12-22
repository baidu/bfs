// Copyright (c) 2014 Baidu.com, Inc. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: qinzuoyan01@baidu.com (Qin Zuoyan)

#ifndef _SOFA_PBRPC_RW_LOCK_H_
#define _SOFA_PBRPC_RW_LOCK_H_

#include <pthread.h>

namespace sofa {
namespace pbrpc {

class RWLock
{
public:
    RWLock()
    {
        pthread_rwlock_init(&_lock, NULL);
    }
    ~RWLock()
    {
        pthread_rwlock_destroy(&_lock);
    }
    void lock()
    {
        pthread_rwlock_wrlock(&_lock);
    }
    void lock_shared()
    {
        pthread_rwlock_rdlock(&_lock);
    }
    void unlock()
    {
        pthread_rwlock_unlock(&_lock);
    }
private:
    pthread_rwlock_t _lock;
};

class ReadLocker
{
public:
    explicit ReadLocker(RWLock* lock) : _lock(lock)
    {
        _lock->lock_shared();
    }
    ~ReadLocker()
    {
        _lock->unlock();
    }
private:
    RWLock* _lock;
};
class WriteLocker
{
public:
    explicit WriteLocker(RWLock* lock) : _lock(lock)
    {
        _lock->lock();
    }
    ~WriteLocker()
    {
        _lock->unlock();
    }
private:
    RWLock* _lock;
};

} // namespace pbrpc
} // namespace sofa

#endif // _SOFA_PBRPC_RW_LOCK_H_

/* vim: set ts=4 sw=4 sts=4 tw=100 */
