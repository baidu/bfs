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

#ifndef _SOFA_PBRPC_SMART_PTR_MEMORY_ORDER_
#define _SOFA_PBRPC_SMART_PTR_MEMORY_ORDER_

namespace sofa {
namespace pbrpc {

//
// Enum values are chosen so that code that needs to insert
// a trailing fence for acquire semantics can use a single
// test such as:
//
// if( mo & memory_order_acquire ) { ...fence... }
//
// For leading fences one can use:
//
// if( mo & memory_order_release ) { ...fence... }
//
// Architectures such as Alpha that need a fence on consume
// can use:
//
// if( mo & ( memory_order_acquire | memory_order_consume ) ) { ...fence... }
//

enum memory_order
{
    memory_order_relaxed = 0,
    memory_order_acquire = 1,
    memory_order_release = 2,
    memory_order_acq_rel = 3, // acquire | release
    memory_order_seq_cst = 7, // acq_rel | 4
    memory_order_consume = 8
};

} // namespace pbrpc
} // namespace sofa

#endif // _SOFA_PBRPC_SMART_PTR_MEMORY_ORDER_

/* vim: set ts=4 sw=4 sts=4 tw=100 */
