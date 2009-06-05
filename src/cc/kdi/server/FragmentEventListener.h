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

#ifndef KDI_SERVER_FRAGMENTEVENTLISTENER_H
#define KDI_SERVER_FRAGMENTEVENTLISTENER_H

#include <kdi/server/Fragment.h>
#include <warp/interval.h>
#include <string>
#include <vector>

namespace kdi {
namespace server {

    enum FragmentEventType {
        FET_NEW_FRAGMENT,
        FET_FRAGMENTS_REPLACED,
    };

    class FragmentEventListener;

} // namespace server
} // namespace kdi

//----------------------------------------------------------------------------
// FragmentEventListener
//----------------------------------------------------------------------------
class kdi::server::FragmentEventListener
{
public:
    virtual void onFragmentEvent(
        warp::Interval<std::string> const & affectedRows,
        FragmentEventType type) = 0;

protected:
    ~FragmentEventListener() {}
};

#endif // KDI_SERVER_FRAGMENTEVENTLISTENER_H
