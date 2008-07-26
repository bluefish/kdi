//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
// Created 2008-07-14
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

#ifndef KDI_TABLET_METACONFIGMANAGER_H
#define KDI_TABLET_METACONFIGMANAGER_H

#include <kdi/cell.h>
#include <kdi/tablet/ConfigManager.h>
#include <boost/thread/mutex.hpp>
#include <boost/noncopyable.hpp>
#include <boost/enable_shared_from_this.hpp>

namespace kdi {
namespace tablet {
    
    /// ConfigManager that stores tablet config information in a
    /// config column in the meta table.  It is all in one cell so it
    /// can be treated as an atomic unit.
    class MetaConfigManager;

    typedef boost::shared_ptr<MetaConfigManager> MetaConfigManagerPtr;

} // namespace tablet
} // namespace kdi

//----------------------------------------------------------------------------
// MetaConfigManager
//----------------------------------------------------------------------------
class kdi::tablet::MetaConfigManager
    : public kdi::tablet::ConfigManager,
      public boost::enable_shared_from_this<kdi::tablet::MetaConfigManager>,
      private boost::noncopyable
{
    std::string rootDir;
    std::string serverName;
    std::string metaTableUri;

    mutable TablePtr _metaTable;
    mutable boost::mutex metaTableMutex;

public:
    MetaConfigManager(std::string const & rootDir,
                      std::string const & serverName,
                      std::string const & metaTableUri);
    ~MetaConfigManager();

    // ConfigManager API
    std::list<TabletConfig> loadTabletConfigs(std::string const & tableName);
    void setTabletConfig(std::string const & tableName, TabletConfig const & cfg);
    std::string getDataFile(std::string const & tableName);
    std::pair<TablePtr, std::string> openTable(std::string const & uri);

    /// Get a ConfigManager adapter for fixed, file-based configs.
    /// This will typically be used to load the root META table.
    ConfigManagerPtr getFixedAdapter();

    TabletConfig getConfigFromCell(Cell const & configCell) const;
    std::string getConfigCellValue(TabletConfig const & config) const;

    std::string const & getServerName() const { return serverName; }
    TablePtr const & getMetaTable() const;

private:
    class FixedAdapter;
    std::string getNewFile(std::string const & tableDir) const;
    std::string const & getRootDir() const { return rootDir; }
};



#endif // KDI_TABLET_METACONFIGMANAGER_H
