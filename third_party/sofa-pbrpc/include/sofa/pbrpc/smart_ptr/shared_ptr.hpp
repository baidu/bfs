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

#ifndef _SOFA_PBRPC_SMART_PTR_SHARED_PTR_
#define _SOFA_PBRPC_SMART_PTR_SHARED_PTR_

#include <sofa/pbrpc/smart_ptr/checked_delete.hpp>
#include <sofa/pbrpc/smart_ptr/detail/shared_count.hpp>
#include <sofa/pbrpc/smart_ptr/detail/sp_convertible.hpp>
#include <sofa/pbrpc/smart_ptr/detail/spinlock_pool.hpp>
#include <sofa/pbrpc/smart_ptr/memory_order.hpp>

#include <algorithm>            // for std::swap
#include <functional>           // for std::less
#include <typeinfo>             // for std::bad_cast
#include <cstddef>              // for std::size_t
#include <iosfwd>               // for std::basic_ostream
#include <ostream>
#include <cassert>

namespace sofa {
namespace pbrpc {

template<class T> class shared_ptr;
template<class T> class weak_ptr;
template<class T> class enable_shared_from_this;
template<class T> class enable_shared_from_this2;

namespace detail
{

struct static_cast_tag {};
struct const_cast_tag {};
struct dynamic_cast_tag {};
struct polymorphic_cast_tag {};

template<class T> struct shared_ptr_traits
{
    typedef T & reference;
};

template<> struct shared_ptr_traits<void>
{
    typedef void reference;
};

template<> struct shared_ptr_traits<void const>
{
    typedef void reference;
};

template<> struct shared_ptr_traits<void volatile>
{
    typedef void reference;
};

template<> struct shared_ptr_traits<void const volatile>
{
    typedef void reference;
};


// enable_shared_from_this support

template< class X, class Y, class T > 
inline void sp_enable_shared_from_this( sofa::pbrpc::shared_ptr<X> const * ppx, Y const * py, sofa::pbrpc::enable_shared_from_this< T > const * pe )
{
    if( pe != 0 )
    {
        pe->_internal_accept_owner( ppx, const_cast< Y* >( py ) );
    }
}

template< class X, class Y, class T > 
inline void sp_enable_shared_from_this( sofa::pbrpc::shared_ptr<X> * ppx, Y const * py, sofa::pbrpc::enable_shared_from_this2< T > const * pe )
{
    if( pe != 0 )
    {
        pe->_internal_accept_owner( ppx, const_cast< Y* >( py ) );
    }
}


struct sp_any_pointer
{
    template<class T> sp_any_pointer( T* ) {}
};

inline void sp_enable_shared_from_this( sp_any_pointer, sp_any_pointer, sp_any_pointer )
{
}


} // namespace detail


//
//  shared_ptr
//
//  An enhanced relative of scoped_ptr with reference counted copy semantics.
//  The object pointed to is deleted when the last shared_ptr pointing to it
//  is destroyed or reset.
//

template<class T> class shared_ptr
{
private:

    typedef shared_ptr<T> this_type;

public:

    typedef T element_type;
    typedef T value_type;
    typedef T * pointer;
    typedef typename sofa::pbrpc::detail::shared_ptr_traits<T>::reference reference;

    shared_ptr(): px(0), pn() // never throws in 1.30+
    {
    }

    template<class Y>
    explicit shared_ptr( Y * p ): px( p ), pn( p ) // Y must be complete
    {
        sofa::pbrpc::detail::sp_enable_shared_from_this( this, p, p );
    }

    //
    // Requirements: D's copy constructor must not throw
    //
    // shared_ptr will release p by calling d(p)
    //

    template<class Y, class D> shared_ptr(Y * p, D d): px(p), pn(p, d)
    {
        sofa::pbrpc::detail::sp_enable_shared_from_this( this, p, p );
    }

    // As above, but with allocator. A's copy constructor shall not throw.

    template<class Y, class D, class A> shared_ptr( Y * p, D d, A a ): px( p ), pn( p, d, a )
    {
        sofa::pbrpc::detail::sp_enable_shared_from_this( this, p, p );
    }

//  generated copy constructor, destructor are fine...

    template<class Y>
    explicit shared_ptr(weak_ptr<Y> const & r): pn(r.pn) // may throw
    {
        // it is now safe to copy r.px, as pn(r.pn) did not throw
        px = r.px;
    }

    template<class Y>
    shared_ptr( weak_ptr<Y> const & r, sofa::pbrpc::detail::sp_nothrow_tag ): px( 0 ), pn( r.pn, sofa::pbrpc::detail::sp_nothrow_tag() ) // never throws
    {
        if( !pn.empty() )
        {
            px = r.px;
        }
    }

    template<class Y>
    shared_ptr( shared_ptr<Y> const & r, typename sofa::pbrpc::detail::sp_enable_if_convertible<Y,T>::type = sofa::pbrpc::detail::sp_empty() )
    : px( r.px ), pn( r.pn ) // never throws
    {
    }

    // aliasing
    template< class Y >
    shared_ptr( shared_ptr<Y> const & r, T * p ): px( p ), pn( r.pn ) // never throws
    {
    }

    template<class Y>
    shared_ptr(shared_ptr<Y> const & r, sofa::pbrpc::detail::static_cast_tag): px(static_cast<element_type *>(r.px)), pn(r.pn)
    {
    }

    template<class Y>
    shared_ptr(shared_ptr<Y> const & r, sofa::pbrpc::detail::const_cast_tag): px(const_cast<element_type *>(r.px)), pn(r.pn)
    {
    }

    template<class Y>
    shared_ptr(shared_ptr<Y> const & r, sofa::pbrpc::detail::dynamic_cast_tag): px(dynamic_cast<element_type *>(r.px)), pn(r.pn)
    {
        if(px == 0) // need to allocate new counter -- the cast failed
        {
            pn = sofa::pbrpc::detail::shared_count();
        }
    }

    template<class Y>
    shared_ptr(shared_ptr<Y> const & r, sofa::pbrpc::detail::polymorphic_cast_tag): px(dynamic_cast<element_type *>(r.px)), pn(r.pn)
    {
        if(px == 0)
        {
            throw std::bad_cast();
        }
    }

    // assignment

    shared_ptr & operator=( shared_ptr const & r ) // never throws
    {
        this_type(r).swap(*this);
        return *this;
    }

    template<class Y>
    shared_ptr & operator=(shared_ptr<Y> const & r) // never throws
    {
        this_type(r).swap(*this);
        return *this;
    }



    void reset() // never throws in 1.30+
    {
        this_type().swap(*this);
    }

    template<class Y> void reset(Y * p) // Y must be complete
    {
        assert(p == 0 || p != px); // catch self-reset errors
        this_type(p).swap(*this);
    }

    template<class Y, class D> void reset( Y * p, D d )
    {
        this_type( p, d ).swap( *this );
    }

    template<class Y, class D, class A> void reset( Y * p, D d, A a )
    {
        this_type( p, d, a ).swap( *this );
    }

    template<class Y> void reset( shared_ptr<Y> const & r, T * p )
    {
        this_type( r, p ).swap( *this );
    }

    reference operator* () const // never throws
    {
        assert(px != 0);
        return *px;
    }

    T * operator-> () const // never throws
    {
        assert(px != 0);
        return px;
    }

    T * get() const // never throws
    {
        return px;
    }

// implicit conversion to "bool"
#include <sofa/pbrpc/smart_ptr/detail/operator_bool.hpp>

    bool unique() const // never throws
    {
        return pn.unique();
    }

    long use_count() const // never throws
    {
        return pn.use_count();
    }

    void swap(shared_ptr<T> & other) // never throws
    {
        std::swap(px, other.px);
        pn.swap(other.pn);
    }

    template<class Y> bool owner_before( shared_ptr<Y> const & rhs ) const
    {
        return pn < rhs.pn;
    }

    template<class Y> bool owner_before( weak_ptr<Y> const & rhs ) const
    {
        return pn < rhs.pn;
    }

    void * _internal_get_deleter( std::type_info const & ti ) const
    {
        return pn.get_deleter( ti );
    }

    bool _internal_equiv( shared_ptr const & r ) const
    {
        return px == r.px && pn == r.pn;
    }


private:

    template<class Y> friend class shared_ptr;
    template<class Y> friend class weak_ptr;

    T * px;                     // contained pointer
    sofa::pbrpc::detail::shared_count pn;    // reference counter

};  // shared_ptr

template<class T, class U> inline bool operator==(shared_ptr<T> const & a, shared_ptr<U> const & b)
{
    return a.get() == b.get();
}

template<class T, class U> inline bool operator!=(shared_ptr<T> const & a, shared_ptr<U> const & b)
{
    return a.get() != b.get();
}

template<class T, class U> inline bool operator<(shared_ptr<T> const & a, shared_ptr<U> const & b)
{
    return a.owner_before( b );
}

template<class T> inline void swap(shared_ptr<T> & a, shared_ptr<T> & b)
{
    a.swap(b);
}

template<class T, class U> shared_ptr<T> static_pointer_cast(shared_ptr<U> const & r)
{
    return shared_ptr<T>(r, sofa::pbrpc::detail::static_cast_tag());
}

template<class T, class U> shared_ptr<T> const_pointer_cast(shared_ptr<U> const & r)
{
    return shared_ptr<T>(r, sofa::pbrpc::detail::const_cast_tag());
}

template<class T, class U> shared_ptr<T> dynamic_pointer_cast(shared_ptr<U> const & r)
{
    return shared_ptr<T>(r, sofa::pbrpc::detail::dynamic_cast_tag());
}

// get_pointer() enables boost::mem_fn to recognize shared_ptr

template<class T> inline T * get_pointer(shared_ptr<T> const & p)
{
    return p.get();
}

// operator<<


template<class Y> std::ostream & operator<< (std::ostream & os, shared_ptr<Y> const & p)
{
    os << p.get();
    return os;
}

template<class E, class T, class Y> std::basic_ostream<E, T> & operator<< (std::basic_ostream<E, T> & os, shared_ptr<Y> const & p)
{
    os << p.get();
    return os;
}

// get_deleter

template<class D, class T> D * get_deleter(shared_ptr<T> const & p)
{
    void const * q = p._internal_get_deleter(typeid(D));
    return const_cast<D *>(static_cast<D const *>(q));
}


template<class T> inline bool atomic_is_lock_free( shared_ptr<T> const * /*p*/ )
{
    return false;
}

template<class T> shared_ptr<T> atomic_load( shared_ptr<T> const * p )
{
    sofa::pbrpc::detail::spinlock_pool<2>::scoped_lock lock( p );
    return *p;
}

template<class T> inline shared_ptr<T> atomic_load_explicit( shared_ptr<T> const * p, ::sofa::pbrpc::memory_order /*mo*/ )
{
    return atomic_load( p );
}

template<class T> void atomic_store( shared_ptr<T> * p, shared_ptr<T> r )
{
    sofa::pbrpc::detail::spinlock_pool<2>::scoped_lock lock( p );
    p->swap( r );
}

template<class T> inline void atomic_store_explicit( shared_ptr<T> * p, shared_ptr<T> r, ::sofa::pbrpc::memory_order /*mo*/ )
{
    atomic_store( p, r ); // std::move( r )
}

template<class T> shared_ptr<T> atomic_exchange( shared_ptr<T> * p, shared_ptr<T> r )
{
    sofa::pbrpc::detail::spinlock & sp = sofa::pbrpc::detail::spinlock_pool<2>::spinlock_for( p );

    sp.lock();
    p->swap( r );
    sp.unlock();

    return r; // return std::move( r )
}

template<class T> shared_ptr<T> atomic_exchange_explicit( shared_ptr<T> * p, shared_ptr<T> r, ::sofa::pbrpc::memory_order /*mo*/ )
{
    return atomic_exchange( p, r ); // std::move( r )
}

template<class T> bool atomic_compare_exchange( shared_ptr<T> * p, shared_ptr<T> * v, shared_ptr<T> w )
{
    sofa::pbrpc::detail::spinlock & sp = sofa::pbrpc::detail::spinlock_pool<2>::spinlock_for( p );

    sp.lock();

    if( p->_internal_equiv( *v ) )
    {
        p->swap( w );

        sp.unlock();

        return true;
    }
    else
    {
        shared_ptr<T> tmp( *p );

        sp.unlock();

        tmp.swap( *v );
        return false;
    }
}

template<class T> inline bool atomic_compare_exchange_explicit( shared_ptr<T> * p, shared_ptr<T> * v, shared_ptr<T> w, ::sofa::pbrpc::memory_order /*success*/, ::sofa::pbrpc::memory_order /*failure*/ )
{
    return atomic_compare_exchange( p, v, w ); // std::move( w )
}

// hash_value

template< class T > std::size_t hash_value( sofa::pbrpc::shared_ptr<T> const & p )
{
    return reinterpret_cast<std::size_t>(p.get());
}

} // namespace pbrpc
} // namespace sofa

#endif // _SOFA_PBRPC_SMART_PTR_SHARED_PTR_

/* vim: set ts=4 sw=4 sts=4 tw=100 */
