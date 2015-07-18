// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Simple hash function used for internal data structures

#ifndef BAIDU_COMMON_HASH_H_
#define BAIDU_COMMON_HASH_H_

#include <stddef.h>
#include <stdint.h>

namespace common {

uint32_t Hash(const char* data, size_t n, uint32_t seed);

} // common

#endif  // BAIDU_COMMON_HASH_H_
