//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-02-27
//
// This file is part of KDI.
//
// KDI is free software; you can redistribute it and/or modify it under the
// terms of the GNU General Public License as published by the Free Software
// Foundation; either version 2 of the License, or any later version.
//
// KDI is distributed in the hope that it will be useful, but WITHOUT ANY
// WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
// FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
// details.
//
// You should have received a copy of the GNU General Public License along
// with this program; if not, write to the Free Software Foundation, Inc.,
// 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.
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
