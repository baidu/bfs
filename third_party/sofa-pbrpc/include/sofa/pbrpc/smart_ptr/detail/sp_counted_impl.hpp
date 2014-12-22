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

#ifndef _SOFA_PBRPC_SMART_PTR_DETAIL_SP_COUNTED_IMPL_
#define _SOFA_PBRPC_SMART_PTR_DETAIL_SP_COUNTED_IMPL_

#include <sofa/pbrpc/smart_ptr/checked_delete.hpp>
#include <sofa/pbrpc/smart_ptr/detail/sp_counted_base.hpp>

#include <memory>           // std::allocator
#include <cstddef>          // std::size_t
#include <typeinfo>         // std::type_info

namespace sofa {
namespace pbrpc {
namespace detail {

template<class X> class sp_counted_impl_p: public sp_counted_base
{
private:

    X * px_;

    sp_counted_impl_p( sp_counted_impl_p const & );
    sp_counted_impl_p & operator= ( sp_counted_impl_p const & );

    typedef sp_counted_impl_p<X> this_type;

public:

    explicit sp_counted_impl_p( X * px ): px_( px )
    {
    }

    virtual void dispose() // nothrow
    {
        sofa::pbrpc::checked_delete( px_ );
    }

    virtual void * get_deleter( std::type_info const & )
    {
        return 0;
    }


    void * operator new( std::size_t )
    {
        return std::allocator<this_type>().allocate( 1, static_cast<this_type *>(0) );
    }

    void operator delete( void * p )
    {
        std::allocator<this_type>().deallocate( static_cast<this_type *>(p), 1 );
    }


};


template<class P, class D> class sp_counted_impl_pd: public sp_counted_base
{
private:

    P ptr; // copy constructor must not throw
    D del; // copy constructor must not throw

    sp_counted_impl_pd( sp_counted_impl_pd const & );
    sp_counted_impl_pd & operator= ( sp_counted_impl_pd const & );

    typedef sp_counted_impl_pd<P, D> this_type;

public:

    // pre: d(p) must not throw

    sp_counted_impl_pd( P p, D & d ): ptr( p ), del( d )
    {
    }

    sp_counted_impl_pd( P p ): ptr( p ), del()
    {
    }

    virtual void dispose() // nothrow
    {
        del( ptr );
    }

    virtual void * get_deleter( std::type_info const & ti )
    {
        return ti == typeid(D) ? &reinterpret_cast<char&>( del ): 0;
    }


    void * operator new( std::size_t )
    {
        return std::allocator<this_type>().allocate( 1, static_cast<this_type *>(0) );
    }

    void operator delete( void * p )
    {
        std::allocator<this_type>().deallocate( static_cast<this_type *>(p), 1 );
    }

};

template<class P, class D, class A> class sp_counted_impl_pda: public sp_counted_base
{
private:

    P p_; // copy constructor must not throw
    D d_; // copy constructor must not throw
    A a_; // copy constructor must not throw

    sp_counted_impl_pda( sp_counted_impl_pda const & );
    sp_counted_impl_pda & operator= ( sp_counted_impl_pda const & );

    typedef sp_counted_impl_pda<P, D, A> this_type;

public:

    // pre: d( p ) must not throw

    sp_counted_impl_pda( P p, D & d, A a ): p_( p ), d_( d ), a_( a )
    {
    }

    sp_counted_impl_pda( P p, A a ): p_( p ), d_(), a_( a )
    {
    }

    virtual void dispose() // nothrow
    {
        d_( p_ );
    }

    virtual void destroy() // nothrow
    {
        typedef typename A::template rebind< this_type >::other A2;

        A2 a2( a_ );

        this->~this_type();
        a2.deallocate( this, 1 );
    }

    virtual void * get_deleter( std::type_info const & ti )
    {
        return ti == typeid(D) ? &reinterpret_cast<char&>( d_ ): 0;
    }
};

} // namespace detail
} // namespace pbrpc
} // namespace sofa

#endif // _SOFA_PBRPC_SMART_PTR_DETAIL_SP_COUNTED_IMPL_

/* vim: set ts=4 sw=4 sts=4 tw=100 */
