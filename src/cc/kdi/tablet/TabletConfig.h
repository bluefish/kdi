//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
// Created 2008-05-23
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

#ifndef KDI_TABLET_TABLETCONFIG_H
#define KDI_TABLET_TABLETCONFIG_H

#include <warp/interval.h>
#include <string>
#include <vector>

namespace kdi {
namespace tablet {

    class TabletConfig;

} // namespace tablet
} // namespace kdi

//----------------------------------------------------------------------------
// TabletConfig
//----------------------------------------------------------------------------
class kdi::tablet::TabletConfig
{
private:
    warp::Interval<std::string> rows;
    std::vector<std::string> uris;

public:
    /// Create a TabletConfig object.  The rows parameter indicates
    /// the row span covered by the Tablet.  The uris parameter should
    /// contain the ordered list of table URIs that make up the
    /// Tablet.  Each table URI should be suitable for passing to
    /// ConfigManager::openTable().
    TabletConfig(warp::Interval<std::string> const & rows,
                 std::vector<std::string> const & uris) :
        rows(rows),
        uris(uris)
    {
    }

    /// Get the ordered list of table URIs that make up the Tablet.
    /// Each table URI is suitable for passing to
    /// ConfigManager::openTable().
    std::vector<std::string> const & getTableUris() const
    {
        return uris;
    }

    /// Get the row range the Tablet should serve.
    warp::Interval<std::string> const & getTabletRows() const
    {
        return rows;
    }
};


#endif // KDI_TABLET_TABLETCONFIG_H
