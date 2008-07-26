//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
// Created 2008-05-17
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

#ifndef KDI_TABLET_CONFIGMANAGER_H
#define KDI_TABLET_CONFIGMANAGER_H

#include <kdi/tablet/forward.h>
#include <warp/interval.h>
#include <warp/config.h>
#include <string>
#include <list>

namespace kdi {
namespace tablet {

    class ConfigManager;

    // Forward declaration
    class TabletConfig;

} // namespace tablet
} // namespace kdi


//----------------------------------------------------------------------------
// ConfigManager
//----------------------------------------------------------------------------
class kdi::tablet::ConfigManager
{
public:
    virtual ~ConfigManager() {}

    /// Load all configs for all tablets in the given table that
    /// belong on this server.
    virtual std::list<TabletConfig>
    loadTabletConfigs(std::string const & tableName) = 0;

    /// Write (or overwrite) the tablet config for the named table.
    virtual void setTabletConfig(std::string const & tableName,
                                 TabletConfig const & cfg) = 0;

    /// Get a unique path for a new data file in the given table's
    /// namespace.  If the returned path exists, it will be a file
    /// that should be overwritten.
    virtual std::string getDataFile(std::string const & tableName) = 0;

    /// Open a table URI.  The ConfigManager returns the table and a
    /// URI that can be used to reopen the table.
    virtual std::pair<TablePtr, std::string>
    openTable(std::string const & uri) = 0;
};

#endif // KDI_TABLET_CONFIGMANAGER_H
