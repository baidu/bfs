// Copyright (c) 2014 Baidu.com, Inc. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: qinzuoyan01@baidu.com (Qin Zuoyan)

#ifndef _SOFA_PBRPC_CLOSURE_HELPER_H_
#define _SOFA_PBRPC_CLOSURE_HELPER_H_

namespace sofa {
namespace pbrpc {

template <typename T>
inline T * get_pointer(T * p)
{
    return p;
}

// delete p in dtor automatically if Enabled is true
template <bool Enabled, typename T>
class ConditionalAutoDeleter
{
public:
    explicit ConditionalAutoDeleter(T* p)
        : m_p(p)
    {
    }
    ~ConditionalAutoDeleter()
    {
        if (Enabled)
        {
            delete m_p;
        }
    }
private:
    ConditionalAutoDeleter(const ConditionalAutoDeleter&);
    ConditionalAutoDeleter& operator=(const ConditionalAutoDeleter&);
private:
    T* m_p;
};

// This is a typetraits object that's used to take an argument type, and
// extract a suitable type for storing and forwarding arguments.
template <typename T>
struct ParamTraits
{
    typedef const T& ForwardType;
    typedef T StorageType;
};

template <typename T>
struct ParamTraits<T&>
{
    typedef T& ForwardType;
    typedef T StorageType;
};

} // namespace pbrpc
} // namespace sofa

#endif // _SOFA_PBRPC_CLOSURE_HELPER_H_

/* vim: set ts=4 sw=4 sts=4 tw=100 */
