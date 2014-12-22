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

#ifndef _SOFA_PBRPC_SMART_PTR_DETAIL_SP_COUNTED_BASE_GCC_X86_
#define _SOFA_PBRPC_SMART_PTR_DETAIL_SP_COUNTED_BASE_GCC_X86_

#include <typeinfo>

namespace sofa {
namespace pbrpc {
namespace detail {

inline int atomic_exchange_and_add( int * pw, int dv )
{
    // int r = *pw;
    // *pw += dv;
    // return r;

    int r;

    __asm__ __volatile__
    (
        "lock\n\t"
        "xadd %1, %0":
        "=m"( *pw ), "=r"( r ): // outputs (%0, %1)
        "m"( *pw ), "1"( dv ): // inputs (%2, %3 == %1)
        "memory", "cc" // clobbers
    );

    return r;
}

inline void atomic_increment( int * pw )
{
    //atomic_exchange_and_add( pw, 1 );

    __asm__
    (
        "lock\n\t"
        "incl %0":
        "=m"( *pw ): // output (%0)
        "m"( *pw ): // input (%1)
        "cc" // clobbers
    );
}

inline int atomic_conditional_increment( int * pw )
{
    // int rv = *pw;
    // if( rv != 0 ) ++*pw;
    // return rv;

    int rv, tmp;

    __asm__
    (
        "movl %0, %%eax\n\t"
        "0:\n\t"
        "test %%eax, %%eax\n\t"
        "je 1f\n\t"
        "movl %%eax, %2\n\t"
        "incl %2\n\t"
        "lock\n\t"
        "cmpxchgl %2, %0\n\t"
        "jne 0b\n\t"
        "1:":
        "=m"( *pw ), "=&a"( rv ), "=&r"( tmp ): // outputs (%0, %1, %2)
        "m"( *pw ): // input (%3)
        "cc" // clobbers
    );

    return rv;
}

class sp_counted_base
{
private:

    sp_counted_base( sp_counted_base const & );
    sp_counted_base & operator= ( sp_counted_base const & );

    int use_count_;        // #shared
    int weak_count_;       // #weak + (#shared != 0)

public:

    sp_counted_base(): use_count_( 1 ), weak_count_( 1 )
    {
    }

    virtual ~sp_counted_base() // nothrow
    {
    }

    // dispose() is called when use_count_ drops to zero, to release
    // the resources managed by *this.

    virtual void dispose() = 0; // nothrow

    // destroy() is called when weak_count_ drops to zero.

    virtual void destroy() // nothrow
    {
        delete this;
    }

    virtual void * get_deleter( std::type_info const & ti ) = 0;

    void add_ref_copy()
    {
        atomic_increment( &use_count_ );
    }

    bool add_ref_lock() // true on success
    {
        return atomic_conditional_increment( &use_count_ ) != 0;
    }

    void release() // nothrow
    {
        if( atomic_exchange_and_add( &use_count_, -1 ) == 1 )
        {
            dispose();
            weak_release();
        }
    }

    void weak_add_ref() // nothrow
    {
        atomic_increment( &weak_count_ );
    }

    void weak_release() // nothrow
    {
        if( atomic_exchange_and_add( &weak_count_, -1 ) == 1 )
        {
            destroy();
        }
    }

    long use_count() const // nothrow
    {
        return static_cast<int const volatile &>( use_count_ );
    }
};

} // namespace detail
} // namespace pbrpc
} // namespace sofa

#endif // _SOFA_PBRPC_SMART_PTR_DETAIL_SP_COUNTED_BASE_GCC_X86_

/* vim: set ts=4 sw=4 sts=4 tw=100 */
