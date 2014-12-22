// Copyright (c) 2014 Baidu.com, Inc. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: qinzuoyan01@baidu.com (Qin Zuoyan)

#ifndef _SOFA_PBRPC_TIMEOUT_MANAGER_H_
#define _SOFA_PBRPC_TIMEOUT_MANAGER_H_

#include <sofa/pbrpc/common.h>
#include <sofa/pbrpc/ext_closure.h>

namespace sofa {
namespace pbrpc {

// Defined in this file.
class TimeoutManager;
typedef sofa::pbrpc::shared_ptr<TimeoutManager> TimeoutManagerPtr;

// Defined in other files.
class TimeoutManagerImpl;

class TimeoutManager
{
public:
    // Timeout id.  Every timeout event will be assigned an unique id.
    typedef uint64 Id;

    // Timeout type.  Used in callback.
    //   - TIMEOUTED : callback triggered when timeout.
    //   - ERASED    : callback triggered when do 'erase()'.
    //   - CLEARED   : callback triggered when do 'clear()' or destructor.
    enum Type {
        TIMEOUTED = 0,
        ERASED = 1,
        CLEARED = 2
    };

    // Callback.  The callback should be a light weight routine, which means you
    // should not do numerous things or block it.
    typedef ExtClosure<void(Id /*id*/, Type /*type*/)> Callback;

    // Contructor.
    TimeoutManager();

    // Destructor. It will invoke clear() to clear all timeout events.
    ~TimeoutManager();

    // Clear all timeout events from the manager.  All associated callbacks will
    // be invoked with type = CLEARED,
    void clear();

    // Add a one-time timeout event that will be triggered once after time of
    // "interval" milli-seconds from now.
    //
    // The "interval" should be no less than 0.
    //
    // The "callback" should be a self delete closure which can be created through
    // NewExtClosure().
    //
    // The "callback" will always be invoked only once, in following cases:
    //   - Timeouted, with type = TIMEOUTED.
    //   - Erased, with type = ERASED.
    //   - Cleared, with type = CLEARED.
    //
    // After callback done, the "callback" closure will be self deleted.
    Id add(int64 interval, Callback* callback);

    // Add a repeating timeout event that will be triggered after each "interval"
    // milli-seconds.
    //
    // The "interval" should be no less than 0.
    //
    // The "callback" should be a permanent closure which can be created through
    // NewPermanentExtClosure().
    //
    // The "callback" will always be invoked, in following cases:
    //   - Timeouted, with type = TIMEOUTED, maybe invoked for multi-times.
    //   - Erased, with type = ERASED.
    //   - Cleared, with type = CLEARED.
    //
    // If callbacked in case of ERASED or CLEARED, the "callback" closure will
    // deleted by the manager.
    Id add_repeating(int64 interval, Callback* callback);

    // Erase a given timeout event, returns true if the event was actually erased
    // and false if it didn't exist.
    //
    // If returns true, the associated callback will be invoked with type = ERASED.
    bool erase(Id id);

private:
    sofa::pbrpc::shared_ptr<TimeoutManagerImpl> _imp;

    SOFA_PBRPC_DISALLOW_EVIL_CONSTRUCTORS(TimeoutManager);
};

} // namespace pbrpc
} // namespace sofa

#endif // _SOFA_PBRPC_TIMEOUT_MANAGER_H_

/* vim: set ts=4 sw=4 sts=4 tw=100 */
