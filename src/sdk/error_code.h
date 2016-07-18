// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//

#ifndef BFS_SDK_ERROR_CODE_H_
#define BFS_SDK_ERROR_CODE_H_

namespace baidu {
namespace bfs {

#define OK 0
#define BAD_PARAMETER -1
#define NO_PERMISSION -2
#define NO_ENOUGH_QUOTA -3
#define NETWORK_UNAVAILABLE -4
#define TIMEOUT -5
#define NO_ENOUGH_SPACE -6
#define OVERLOAD -7
#define META_NOT_AVAILABLE -8
#define UNKNOWN_ERROR -9

const char* SdkErrorCodeToString(int error_code);

}
}

#endif //BFS_SDK_ERROR_CODE_H_
