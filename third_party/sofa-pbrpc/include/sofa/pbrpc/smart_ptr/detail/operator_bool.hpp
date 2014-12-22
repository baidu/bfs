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

    typedef T * (this_type::*unspecified_bool_type)() const;

    operator unspecified_bool_type() const // never throws
    {
        return px == 0 ? 0: &this_type::get;
    }

    // operator! is redundant, but some compilers need it
    bool operator! () const // never throws
    {
        return px == 0;
    }

/* vim: set ts=4 sw=4 sts=4 tw=100 */
