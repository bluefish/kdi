//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
// Created 2008-05-23
//
// This file is part of KDI.
// 
// KDI is free software; you can redistribute it and/or modify it
// under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// any later version.
// 
// KDI is distributed in the hope that it will be useful, but WITHOUT
// ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
// or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public
// License for more details.
// 
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
// 02110-1301, USA.
//----------------------------------------------------------------------------

#ifndef KDI_TABLET_TABLETCONFIG_H
#define KDI_TABLET_TABLETCONFIG_H

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
public:
    typedef std::vector<std::string> uri_vec_t;

private:
    uri_vec_t tableUris;

public:
    /// Get the ordered list of table URIs that make up the Tablet.
    /// Each table URI is suitable for passing to
    /// ConfigManager::openTable().
    uri_vec_t const & getTableUris() const
    {
        return tableUris;
    }

    /// Set the ordered list of table URIs that make up the Tablet.
    /// Each table URI should be suitable for passing to
    /// ConfigManager::openTable().
    void setTableUris(uri_vec_t const & uris)
    {
        tableUris = uris;
    }
};


#endif // KDI_TABLET_TABLETCONFIG_H
