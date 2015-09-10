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

namespace common {

template <typename Item>
class SlidingWindow {
public:
    typedef boost::function<void (int32_t, Item)> SlidingCallback;
    SlidingWindow(int32_t size, SlidingCallback callback)
      : bitmap_(NULL), items_(NULL), item_count_(0), 
        callback_(callback), size_(size),
        base_offset_(0), ready_(0), notifying_(false) {
        bitmap_ = new char[size];
        memset(bitmap_, 0, size);
        items_ = new Item[size];
        size_ = size;
    }
    ~SlidingWindow() {
        delete[] bitmap_;
        delete[] items_;
    }
    int32_t Size() const {
        return item_count_;
    }
    int32_t GetBaseOffset() const {
        return base_offset_;
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
        mu_.AssertHeld();
        notifying_ = true;
        while (bitmap_[ready_] == 1) {
            mu_.Unlock();
            callback_(base_offset_, items_[ready_]);
            mu_.Lock("SlidingWindow::Notify relock");
            bitmap_[ready_] = 0;
            ++ready_;
            ++base_offset_;
            --item_count_;
            if (ready_ >= size_) {
                ready_ = 0;
            }
        }
        notifying_ = false;
    }
    int32_t UpBound() const {
        return base_offset_ + size_ - 1;
    }
    /// Add a new item to slinding window.
    //  Returns:
    ///     0, Add to receiving buf;
    ///     1, Already received
    ///    -1, Not in receiving window
    /// Notes:
    ///     There is no thread pool, so SlidingCallback would be called by Add.
    ///     Pay attention to a deadlock.
    int Add(int32_t offset, Item item) {
        MutexLock lock(&mu_, "Slinding Add", 50000);
        int32_t pos = offset - base_offset_;
        if (pos >= size_) {
            return -1;
        } else if (pos < 0) {
            return 1;
        }
        pos = (pos + ready_) % size_;
        if (bitmap_[pos]) {
            return 1;
        }
        bitmap_[pos] = 1;
        items_[pos] = item;
        ++item_count_;
        if (!notifying_) Notify();
        return 0;
    }
private:
    char* bitmap_;
    Item* items_;
    int32_t item_count_;
    SlidingCallback callback_;
    int32_t size_;
    int32_t base_offset_;
    int32_t ready_;
    bool notifying_;
    Mutex mu_;
};

} // namespace common

#endif  // COMMON_SLIDING_WINDOW_H_

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
