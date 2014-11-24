// Copyright (c) 2014, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#include "sliding_window.h"
#include <stdlib.h>
#include <boost/bind.hpp>
#include <set>

void Print(const char* buf) {
    printf("%s\n", buf);
}
int main() {
    SlidingWindow<char*> sw(100, &Print);
    
    std::set<int> s;
    for (int i = 0; i < 10000; i++) {
        int t = rand()%500;
       if (s.find(t) == s.end()) {
           char* buf = new char[16];
           snprintf(buf, 16, "%d", t);

           if (sw.Add(t, buf)) {
               printf("Add %d\n", t);
               s.insert(t);
           }
       }
    }
}
















/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
