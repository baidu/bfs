// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//

#include "error_code.h"

namespace baidu {
namespace bfs {

#define MAKE_CASE(name) case name: return (#name)

const char* SdkErrorCodeToString(int error_code) {
    switch (error_code) {
        MAKE_CASE(OK);
        MAKE_CASE(BAD_PARAMETER);
        MAKE_CASE(PERMISSION_DENIED);
        MAKE_CASE(NOT_ENOUGH_QUOTA);
        MAKE_CASE(NETWORK_UNAVAILABLE);
        MAKE_CASE(TIMEOUT);
        MAKE_CASE(NOT_ENOUGH_SPACE);
        MAKE_CASE(OVERLOAD);
        MAKE_CASE(META_NOT_AVAILABLE);
        MAKE_CASE(UNKNOWN_ERROR);
    }
}

}
}
