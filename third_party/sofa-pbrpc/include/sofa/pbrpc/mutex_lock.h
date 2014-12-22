// Copyright (c) 2014 Baidu.com, Inc. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: qinzuoyan01@baidu.com (Qin Zuoyan)

#ifndef _SOFA_PBRPC_MUTEX_LOCK_H_
#define _SOFA_PBRPC_MUTEX_LOCK_H_

#include <pthread.h>

namespace sofa {
namespace pbrpc {
class ConditionVariable;

class MutexLock
{
public:
    MutexLock()
    {
        pthread_mutex_init(&_lock, NULL);
    }
    ~MutexLock()
    {
        pthread_mutex_destroy(&_lock);
    }
    void lock()
    {
        pthread_mutex_lock(&_lock);
    }
    void unlock()
    {
        pthread_mutex_unlock(&_lock);
    }
private:
    friend class ConditionVariable;
    pthread_mutex_t _lock;
};

} // namespace pbrpc
} // namespace sofa

#endif // _SOFA_PBRPC_MUTEX_LOCK_H_

/* vim: set ts=4 sw=4 sts=4 tw=100 */
