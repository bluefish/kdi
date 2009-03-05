//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: kdi/server/TabletEventListener.h $
//
// Created 2009/02/27
//
// Copyright 2009 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef KDI_SERVER_TABLETEVENTLISTENER_H
#define KDI_SERVER_TABLETEVENTLISTENER_H

#include <warp/interval.h>
#include <string>

namespace kdi {
namespace server {

    class TabletEventListener;

} // namespace server
} // namespace kdi

//----------------------------------------------------------------------------
// TabletEventListener
//----------------------------------------------------------------------------
class kdi::server::TabletEventListener
{
public:
    /// Event: the tablet formerly covering the union of 'lo' and 'hi'
    /// has been split into the given sub-ranges.
    virtual void onTabletSplit(
        warp::Interval<std::string> const & lo,
        warp::Interval<std::string> const & hi) = 0;

    /// Event: the formerly separate tablets covering 'lo' and 'hi'
    /// have been merged into a single tablet.
    virtual void onTabletMerge(
        warp::Interval<std::string> const & lo,
        warp::Interval<std::string> const & hi) = 0;

    /// Event: the tablet covering the range 'r' has been dropped from
    /// the server.
    virtual void onTabletDrop(
        warp::Interval<std::string> const & r) = 0;

protected:
    ~TabletEventListener() {}
};

#endif // KDI_SERVER_TABLETEVENTLISTENER_H
