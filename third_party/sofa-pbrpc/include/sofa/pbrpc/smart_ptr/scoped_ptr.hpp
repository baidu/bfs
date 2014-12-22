// Copyright (c) 2014 Baidu.com, Inc. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: qinzuoyan01@baidu.com (Qin Zuoyan)

// This file is modified from boost.
//
// Copyright Beman Dawes 2002, 2006
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//
// See library home page at http://www.boost.org/libs/system

#ifndef _SOFA_PBRPC_SMART_PTR_SCOPED_PTR_
#define _SOFA_PBRPC_SMART_PTR_SCOPED_PTR_

#include <sofa/pbrpc/smart_ptr/checked_delete.hpp>

namespace sofa {
namespace pbrpc {

//  scoped_ptr mimics a built-in pointer except that it guarantees deletion
//  of the object pointed to, either on destruction of the scoped_ptr or via
//  an explicit reset(). scoped_ptr is a simple solution for simple needs;
//  use shared_ptr or std::auto_ptr if your needs are more complex.

template<class T> class scoped_ptr // noncopyable
{
private:

    T * px;

    scoped_ptr(scoped_ptr const &);
    scoped_ptr & operator=(scoped_ptr const &);

    typedef scoped_ptr<T> this_type;

    void operator==( scoped_ptr const& ) const;
    void operator!=( scoped_ptr const& ) const;

public:

    typedef T element_type;

    explicit scoped_ptr( T * p = 0 ): px( p ) // never throws
    {
    }

    ~scoped_ptr() // never throws
    {
        sofa::pbrpc::checked_delete( px );
    }

    void reset(T * p = 0) // never throws
    {
        this_type(p).swap(*this);
    }

    T & operator*() const // never throws
    {
        return *px;
    }

    T * operator->() const // never throws
    {
        return px;
    }

    T * get() const // never throws
    {
        return px;
    }

// implicit conversion to "bool"
#include <sofa/pbrpc/smart_ptr/detail/operator_bool.hpp>

    void swap(scoped_ptr & b) // never throws
    {
        T * tmp = b.px;
        b.px = px;
        px = tmp;
    }
};

template<class T> inline void swap(scoped_ptr<T> & a, scoped_ptr<T> & b) // never throws
{
    a.swap(b);
}

// get_pointer(p) is a generic way to say p.get()

template<class T> inline T * get_pointer(scoped_ptr<T> const & p)
{
    return p.get();
}

} // namespace pbrpc
} // namespace sofa

#endif // _SOFA_PBRPC_SMART_PTR_SCOPED_PTR_

/* vim: set ts=4 sw=4 sts=4 tw=100 */
