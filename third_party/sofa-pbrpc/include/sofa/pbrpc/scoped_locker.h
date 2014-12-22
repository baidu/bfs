// Copyright (c) 2014 Baidu.com, Inc. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: qinzuoyan01@baidu.com (Qin Zuoyan)

#ifndef _SOFA_PBRPC_SCOPED_LOCKER_H_
#define _SOFA_PBRPC_SCOPED_LOCKER_H_

namespace sofa {
namespace pbrpc {

template <typename LockType>
class ScopedLocker
{
public:
    explicit ScopedLocker(LockType& lock)
        : _lock(&lock)
    {
        _lock->lock();
    }

    explicit ScopedLocker(LockType* lock)
        : _lock(lock)
    {
        _lock->lock();
    }

    ~ScopedLocker()
    {
        _lock->unlock();
    }

private:
    LockType* _lock;
}; // class ScopedLocker

} // namespace pbrpc
} // namespace sofa

#endif // _SOFA_PBRPC_SCOPED_LOCKER_H_

/* vim: set ts=4 sw=4 sts=4 tw=100 */
