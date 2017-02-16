// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//

#ifndef BFS_SDK_NONCOPYABLE_H_
#define BFS_SDK_NONCOPYABLE_H_

namespace baidu {
namespace bfs {

class noncopyable {
public:
	noncopyable(){}
	~noncopyable(){}
private:
	noncopyable(const noncopyable&){}
	const noncopyable& operator=(const noncopyable&){}
};

} // namespace bfs
} // namespace baidu

#endif
