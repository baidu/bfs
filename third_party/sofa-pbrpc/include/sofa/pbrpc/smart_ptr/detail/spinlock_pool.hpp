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

#ifndef _SOFA_PBRPC_SMART_PTR_DETAIL_SPINLOCK_POOL_
#define _SOFA_PBRPC_SMART_PTR_DETAIL_SPINLOCK_POOL_

//
//  spinlock_pool<0> is reserved for atomic<>, when/if it arrives
//  spinlock_pool<1> is reserved for shared_ptr reference counts
//  spinlock_pool<2> is reserved for shared_ptr atomic access
//

#include <sofa/pbrpc/smart_ptr/detail/spinlock.hpp>

#include <cstddef>

namespace sofa {
namespace pbrpc {
namespace detail {

template< int I > class spinlock_pool
{
private:

    static spinlock pool_[ 41 ];

public:

    static spinlock & spinlock_for( void const * pv )
    {
        std::size_t i = reinterpret_cast< std::size_t >( pv ) % 41;
        return pool_[ i ];
    }

    class scoped_lock
    {
    private:

        spinlock & sp_;

        scoped_lock( scoped_lock const & );
        scoped_lock & operator=( scoped_lock const & );

    public:

        explicit scoped_lock( void const * pv ): sp_( spinlock_for( pv ) )
        {
            sp_.lock();
        }

        ~scoped_lock()
        {
            sp_.unlock();
        }
    };
};

template< int I > spinlock spinlock_pool< I >::pool_[ 41 ] =
{
    SOFA_PBRPC_SMART_PTR_DETAIL_SPINLOCK_INIT, SOFA_PBRPC_SMART_PTR_DETAIL_SPINLOCK_INIT, SOFA_PBRPC_SMART_PTR_DETAIL_SPINLOCK_INIT, SOFA_PBRPC_SMART_PTR_DETAIL_SPINLOCK_INIT, SOFA_PBRPC_SMART_PTR_DETAIL_SPINLOCK_INIT, 
    SOFA_PBRPC_SMART_PTR_DETAIL_SPINLOCK_INIT, SOFA_PBRPC_SMART_PTR_DETAIL_SPINLOCK_INIT, SOFA_PBRPC_SMART_PTR_DETAIL_SPINLOCK_INIT, SOFA_PBRPC_SMART_PTR_DETAIL_SPINLOCK_INIT, SOFA_PBRPC_SMART_PTR_DETAIL_SPINLOCK_INIT, 
    SOFA_PBRPC_SMART_PTR_DETAIL_SPINLOCK_INIT, SOFA_PBRPC_SMART_PTR_DETAIL_SPINLOCK_INIT, SOFA_PBRPC_SMART_PTR_DETAIL_SPINLOCK_INIT, SOFA_PBRPC_SMART_PTR_DETAIL_SPINLOCK_INIT, SOFA_PBRPC_SMART_PTR_DETAIL_SPINLOCK_INIT, 
    SOFA_PBRPC_SMART_PTR_DETAIL_SPINLOCK_INIT, SOFA_PBRPC_SMART_PTR_DETAIL_SPINLOCK_INIT, SOFA_PBRPC_SMART_PTR_DETAIL_SPINLOCK_INIT, SOFA_PBRPC_SMART_PTR_DETAIL_SPINLOCK_INIT, SOFA_PBRPC_SMART_PTR_DETAIL_SPINLOCK_INIT, 
    SOFA_PBRPC_SMART_PTR_DETAIL_SPINLOCK_INIT, SOFA_PBRPC_SMART_PTR_DETAIL_SPINLOCK_INIT, SOFA_PBRPC_SMART_PTR_DETAIL_SPINLOCK_INIT, SOFA_PBRPC_SMART_PTR_DETAIL_SPINLOCK_INIT, SOFA_PBRPC_SMART_PTR_DETAIL_SPINLOCK_INIT, 
    SOFA_PBRPC_SMART_PTR_DETAIL_SPINLOCK_INIT, SOFA_PBRPC_SMART_PTR_DETAIL_SPINLOCK_INIT, SOFA_PBRPC_SMART_PTR_DETAIL_SPINLOCK_INIT, SOFA_PBRPC_SMART_PTR_DETAIL_SPINLOCK_INIT, SOFA_PBRPC_SMART_PTR_DETAIL_SPINLOCK_INIT, 
    SOFA_PBRPC_SMART_PTR_DETAIL_SPINLOCK_INIT, SOFA_PBRPC_SMART_PTR_DETAIL_SPINLOCK_INIT, SOFA_PBRPC_SMART_PTR_DETAIL_SPINLOCK_INIT, SOFA_PBRPC_SMART_PTR_DETAIL_SPINLOCK_INIT, SOFA_PBRPC_SMART_PTR_DETAIL_SPINLOCK_INIT, 
    SOFA_PBRPC_SMART_PTR_DETAIL_SPINLOCK_INIT, SOFA_PBRPC_SMART_PTR_DETAIL_SPINLOCK_INIT, SOFA_PBRPC_SMART_PTR_DETAIL_SPINLOCK_INIT, SOFA_PBRPC_SMART_PTR_DETAIL_SPINLOCK_INIT, SOFA_PBRPC_SMART_PTR_DETAIL_SPINLOCK_INIT, 
    SOFA_PBRPC_SMART_PTR_DETAIL_SPINLOCK_INIT
};

} // namespace detail
} // namespace pbrpc
} // namespace sofa

#endif // _SOFA_PBRPC_SMART_PTR_DETAIL_SPINLOCK_POOL_

/* vim: set ts=4 sw=4 sts=4 tw=100 */
