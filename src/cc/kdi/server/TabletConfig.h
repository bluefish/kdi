//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-03-13
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

#ifndef KDI_SERVER_TABLETCONFIG_H
#define KDI_SERVER_TABLETCONFIG_H

#include <warp/interval.h>
#include <boost/shared_ptr.hpp>
#include <vector>
#include <string>

namespace kdi {
namespace server {

    struct TabletConfig
    {
        struct Fragment
        {
            // Location of fragment, relative to data root
            std::string filename;
            // Column families to use from the fragment
            std::vector<std::string> families;
        };

        std::string tableName;
        warp::Interval<std::string> rows;
        std::vector<Fragment> fragments;
        std::string log;
        std::string location;

        std::string getTabletName() const;

        bool empty() const
        {
            return fragments.empty() && log.empty() && location.empty();
        }
    };

    typedef boost::shared_ptr<TabletConfig> TabletConfigPtr;
    typedef boost::shared_ptr<TabletConfig const> TabletConfigCPtr;

} // namespace server
} // namespace kdi

#endif // KDI_SERVER_TABLETCONFIG_H
