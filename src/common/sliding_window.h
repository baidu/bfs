// Copyright (c) 2014, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#ifndef  COMMON_SLIDING_WINDOW_H_
#define  COMMON_SLIDING_WINDOW_H_

#include <stdint.h>
#include <vector>
#include <map>
#include <boost/function.hpp>

#include "mutex.h"

template <typename Item>
class SlidingWindow {
public:
    typedef boost::function<void (int32_t, const Item)> SlidingCallback;
    SlidingWindow(int32_t size, SlidingCallback callback)
      : bitmap_(NULL), items_(NULL),
        callback_(callback), size_(size),
        base_offset_(0), ready_(0) {
        bitmap_ = new char[size];
        memset(bitmap_, 0, size);
        items_ = new Item[size];
        size_ = size;
    }
    ~SlidingWindow() {
        delete[] bitmap_;
        delete[] items_;
    }
    void GetFragments(std::vector<std::pair<int32_t, Item> >* fragments) {
        MutexLock lock(&mu_);
        for (int i = 0; i < size_; i++) {
            if (bitmap_[(ready_ + i) % size_]) {
                fragments->push_back(std::make_pair(base_offset_+i, items_[(ready_ + i) % size_]));
            }
        }
    }
    void Notify() {
        while (bitmap_[ready_] == 1) {
            callback_(base_offset_, items_[ready_]);

            bitmap_[ready_] = 0;
            ready_ ++;
            base_offset_ ++;
            if (ready_ >= size_) {
                ready_ = 0;
            }
        }
    }
    bool Add(int32_t offset, Item item) {
        MutexLock lock(&mu_);
        int32_t pos = offset - base_offset_;
        if (pos >= size_) {
            return false;
        } else if (pos < 0) {
            return true;
        }
        pos = (pos + ready_) % size_;
        if (bitmap_[pos]) {
            return true;
        }
        bitmap_[pos] = 1;
        items_[pos] = item;
        Notify();
        return true;
    }
private:
    char* bitmap_;
    Item* items_;
    SlidingCallback callback_;
    int32_t size_;
    int32_t base_offset_;
    int32_t ready_;
    Mutex mu_;
};




#endif  // COMMON_SLIDING_WINDOW_H_

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
