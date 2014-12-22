// Copyright (c) 2014 Baidu.com, Inc. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: qinzuoyan01@baidu.com (Qin Zuoyan)

#ifndef _SOFA_PBRPC_WAIT_EVENT_H_
#define _SOFA_PBRPC_WAIT_EVENT_H_

#include <pthread.h>

namespace sofa {
namespace pbrpc {

class WaitEvent
{
public:
    WaitEvent() : _signaled(false)
    {
        pthread_mutex_init(&_lock, NULL);
        pthread_cond_init(&_cond, NULL);
    }
    ~WaitEvent() 
    {
        pthread_mutex_destroy(&_lock);
        pthread_cond_destroy(&_cond);
    }

    void Wait()
    {
        pthread_mutex_lock(&_lock);
        while (!_signaled)
        {
            pthread_cond_wait(&_cond, &_lock);
        }
        _signaled = false;
        pthread_mutex_unlock(&_lock);
    }

    void Signal()
    {
        pthread_mutex_lock(&_lock);
        _signaled = true;
        pthread_cond_signal(&_cond);
        pthread_mutex_unlock(&_lock);
    }

private:
    pthread_mutex_t _lock;
    pthread_cond_t _cond;
    bool _signaled;
};

} // namespace pbrpc
} // namespace sofa

#endif // _SOFA_PBRPC_WAIT_EVENT_H_

/* vim: set ts=4 sw=4 sts=4 tw=100 */
