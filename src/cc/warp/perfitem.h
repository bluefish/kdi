//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-01-15
// 
// This file is part of the warp library.
// 
// The warp library is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by the
// Free Software Foundation; either version 2 of the License, or any later
// version.
// 
// The warp library is distributed in the hope that it will be useful, but
// WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General
// Public License for more details.
// 
// You should have received a copy of the GNU General Public License along
// with this program; if not, write to the Free Software Foundation, Inc.,
// 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.
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
