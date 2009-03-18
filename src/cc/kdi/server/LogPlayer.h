//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-03-17
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

#ifndef KDI_SERVER_LOGPLAYER_H
#define KDI_SERVER_LOGPLAYER_H

#include <kdi/strref.h>
#include <warp/interval.h>
#include <string>
#include <vector>
#include <utility>

namespace kdi {
namespace server {

    class LogPlayer;

} // namespace server
} // namespace kdi


//----------------------------------------------------------------------------
// LogPlayer
//----------------------------------------------------------------------------
class kdi::server::LogPlayer
{
public:
    typedef warp::Interval<std::string> row_interval;
    typedef std::pair<std::string, row_interval> tablet_pair;
    typedef std::vector<tablet_pair> filter_vec;

public:
    virtual void replay(strref_t logDir, filter_vec const & replayFilter) = 0;

protected:
    ~LogPlayer() {}
};

#endif // KDI_SERVER_LOGPLAYER_H
