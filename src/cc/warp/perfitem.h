//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/warp/perfitem.h#1 $
//
// Created 2007/01/15
//
// Copyright 2007 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef WARP_PERFITEM_H
#define WARP_PERFITEM_H

#include <stdint.h>
#include <stddef.h>
#include <string>
#include <boost/utility.hpp>

namespace warp
{
    struct PerformanceInfo;
    class PerformanceItem;

    // Defined in perflog.h
    class PerformanceLog;
}


//----------------------------------------------------------------------------
// PerformanceInfo
//----------------------------------------------------------------------------
struct warp::PerformanceInfo
{
    enum
    {
        EVENT_FIRST = 1,     // set on first getMessage() after adding
        EVENT_LAST  = 2,     // set on last getMessage() before removing
    };

    int64_t currentTimeNs;
    int64_t deltaTimeNs;
    int64_t flags;
};


//----------------------------------------------------------------------------
// PerformanceItem
//----------------------------------------------------------------------------
class warp::PerformanceItem : private boost::noncopyable
{
    PerformanceLog * tracker;
    size_t trackerIndex;
    bool inTeardown;

    friend class PerformanceLog;

public:
    PerformanceItem();
    virtual ~PerformanceItem();

    /// Return true iff this item is associated with a tracker.  This
    /// function is not synchronized.
    bool isTracked() const;

    /// Remove self from associated tracker, if any.  If the item
    /// isn't being tracked, do nothing.
    void removeSelf();

    /// Get the TYPE part of the reporting line for this item.
    virtual char const * getType() const = 0;

    /// Get the MESSAGE part of the reporting line for this item.  The
    /// message is stored in \c msg, without newlines.  The caller
    /// provides \c info for context when generating the message.
    /// @return true iff the message should be displayed
    virtual bool getMessage(std::string & msg, PerformanceInfo const & info) = 0;
};


#endif // WARP_PERFITEM_H
