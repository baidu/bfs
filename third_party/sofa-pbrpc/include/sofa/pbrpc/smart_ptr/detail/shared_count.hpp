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

#ifndef _SOFA_PBRPC_SMART_PTR_DETAIL_SHARED_COUNT_
#define _SOFA_PBRPC_SMART_PTR_DETAIL_SHARED_COUNT_

#include <sofa/pbrpc/smart_ptr/checked_delete.hpp>
#include <sofa/pbrpc/smart_ptr/bad_weak_ptr.hpp>
#include <sofa/pbrpc/smart_ptr/detail/sp_counted_base.hpp>
#include <sofa/pbrpc/smart_ptr/detail/sp_counted_impl.hpp>

#include <functional>       // std::less
#include <new>              // std::bad_alloc

namespace sofa {
namespace pbrpc {
namespace detail {

int const shared_count_id = 0x2C35F101;
int const   weak_count_id = 0x298C38A4;

struct sp_nothrow_tag {};

template< class D > struct sp_inplace_tag
{
};

class weak_count;

class shared_count
{
private:

    sp_counted_base * pi_;

    friend class weak_count;

public:

    shared_count(): pi_(0) // nothrow
    {
    }

    template<class Y> explicit shared_count( Y * p ): pi_( 0 )
    {
        try
        {
            pi_ = new sp_counted_impl_p<Y>( p );
        }
        catch(...)
        {
            sofa::pbrpc::checked_delete( p );
            throw;
        }

    }

    template<class P, class D> shared_count( P p, D d ): pi_(0)
    {

        try
        {
            pi_ = new sp_counted_impl_pd<P, D>(p, d);
        }
        catch(...)
        {
            d(p); // delete p
            throw;
        }

    }

    template< class P, class D > shared_count( P p, sp_inplace_tag<D> ): pi_( 0 )
    {

        try
        {
            pi_ = new sp_counted_impl_pd< P, D >( p );
        }
        catch( ... )
        {
            D()( p ); // delete p
            throw;
        }

    }

    template<class P, class D, class A> shared_count( P p, D d, A a ): pi_( 0 )
    {
        typedef sp_counted_impl_pda<P, D, A> impl_type;
        typedef typename A::template rebind< impl_type >::other A2;

        A2 a2( a );

        try
        {
            pi_ = a2.allocate( 1, static_cast< impl_type* >( 0 ) );
            new( static_cast< void* >( pi_ ) ) impl_type( p, d, a );
        }
        catch(...)
        {
            d( p );

            if( pi_ != 0 )
            {
                a2.deallocate( static_cast< impl_type* >( pi_ ), 1 );
            }

            throw;
        }

    }

    template< class P, class D, class A > shared_count( P p, sp_inplace_tag< D >, A a ): pi_( 0 )
    {
        typedef sp_counted_impl_pda< P, D, A > impl_type;
        typedef typename A::template rebind< impl_type >::other A2;

        A2 a2( a );

        try
        {
            pi_ = a2.allocate( 1, static_cast< impl_type* >( 0 ) );
            new( static_cast< void* >( pi_ ) ) impl_type( p, a );
        }
        catch(...)
        {
            D()( p );

            if( pi_ != 0 )
            {
                a2.deallocate( static_cast< impl_type* >( pi_ ), 1 );
            }

            throw;
        }

    }

    ~shared_count() // nothrow
    {
        if( pi_ != 0 ) pi_->release();
    }

    shared_count(shared_count const & r): pi_(r.pi_) // nothrow
    {
        if( pi_ != 0 ) pi_->add_ref_copy();
    }

    explicit shared_count(weak_count const & r); // throws bad_weak_ptr when r.use_count() == 0
    shared_count( weak_count const & r, sp_nothrow_tag ); // constructs an empty *this when r.use_count() == 0

    shared_count & operator= (shared_count const & r) // nothrow
    {
        sp_counted_base * tmp = r.pi_;

        if( tmp != pi_ )
        {
            if( tmp != 0 ) tmp->add_ref_copy();
            if( pi_ != 0 ) pi_->release();
            pi_ = tmp;
        }

        return *this;
    }

    void swap(shared_count & r) // nothrow
    {
        sp_counted_base * tmp = r.pi_;
        r.pi_ = pi_;
        pi_ = tmp;
    }

    long use_count() const // nothrow
    {
        return pi_ != 0? pi_->use_count(): 0;
    }

    bool unique() const // nothrow
    {
        return use_count() == 1;
    }

    bool empty() const // nothrow
    {
        return pi_ == 0;
    }

    friend inline bool operator==(shared_count const & a, shared_count const & b)
    {
        return a.pi_ == b.pi_;
    }

    friend inline bool operator<(shared_count const & a, shared_count const & b)
    {
        return std::less<sp_counted_base *>()( a.pi_, b.pi_ );
    }

    void * get_deleter( std::type_info const & ti ) const
    {
        return pi_? pi_->get_deleter( ti ): 0;
    }
};


class weak_count
{
private:

    sp_counted_base * pi_;

    friend class shared_count;

public:

    weak_count(): pi_(0) // nothrow
    {
    }

    weak_count(shared_count const & r): pi_(r.pi_) // nothrow
    {
        if(pi_ != 0) pi_->weak_add_ref();
    }

    weak_count(weak_count const & r): pi_(r.pi_) // nothrow
    {
        if(pi_ != 0) pi_->weak_add_ref();
    }

    ~weak_count() // nothrow
    {
        if(pi_ != 0) pi_->weak_release();
    }

    weak_count & operator= (shared_count const & r) // nothrow
    {
        sp_counted_base * tmp = r.pi_;

        if( tmp != pi_ )
        {
            if(tmp != 0) tmp->weak_add_ref();
            if(pi_ != 0) pi_->weak_release();
            pi_ = tmp;
        }

        return *this;
    }

    weak_count & operator= (weak_count const & r) // nothrow
    {
        sp_counted_base * tmp = r.pi_;

        if( tmp != pi_ )
        {
            if(tmp != 0) tmp->weak_add_ref();
            if(pi_ != 0) pi_->weak_release();
            pi_ = tmp;
        }

        return *this;
    }

    void swap(weak_count & r) // nothrow
    {
        sp_counted_base * tmp = r.pi_;
        r.pi_ = pi_;
        pi_ = tmp;
    }

    long use_count() const // nothrow
    {
        return pi_ != 0? pi_->use_count(): 0;
    }

    bool empty() const // nothrow
    {
        return pi_ == 0;
    }

    friend inline bool operator==(weak_count const & a, weak_count const & b)
    {
        return a.pi_ == b.pi_;
    }

    friend inline bool operator<(weak_count const & a, weak_count const & b)
    {
        return std::less<sp_counted_base *>()(a.pi_, b.pi_);
    }
};

inline shared_count::shared_count( weak_count const & r ): pi_( r.pi_ )
{
    if( pi_ == 0 || !pi_->add_ref_lock() )
    {
        throw sofa::pbrpc::bad_weak_ptr();
    }
}

inline shared_count::shared_count( weak_count const & r, sp_nothrow_tag ): pi_( r.pi_ )
{
    if( pi_ != 0 && !pi_->add_ref_lock() )
    {
        pi_ = 0;
    }
}

} // namespace detail
} // namespace pbrpc
} // namespace sofa

#endif // _SOFA_PBRPC_SMART_PTR_DETAIL_SHARED_COUNT_

/* vim: set ts=4 sw=4 sts=4 tw=100 */
