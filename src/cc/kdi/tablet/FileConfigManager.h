//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
// Created 2008-05-21
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

#ifndef KDI_TABLET_FILECONFIGMANAGER_H
#define KDI_TABLET_FILECONFIGMANAGER_H

#include <kdi/tablet/ConfigManager.h>

namespace kdi {
namespace tablet {

    class FileConfigManager;

} // namespace tablet
} // namespace kdi


//----------------------------------------------------------------------------
// FileConfigManager
//----------------------------------------------------------------------------
class kdi::tablet::FileConfigManager
    : public kdi::tablet::ConfigManager
{
    /// Per-directory config management
    class DirInfo;

    /// Overall directory map
    class DirMap
    {
        typedef boost::shared_ptr<DirInfo> dir_ptr_t;
        typedef std::map<std::string, dir_ptr_t> map_t;

        std::string rootDir;
        map_t infos;

    public:
        explicit DirMap(std::string const & rootDir);
        DirInfo & getDir(std::string const & name);
    };

    warp::Synchronized<DirMap> syncDirMap;

public:
    explicit FileConfigManager(std::string const & rootDir);
    ~FileConfigManager();

    // ConfigManager API
    virtual TabletConfig getTabletConfig(std::string const & tabletName);
    virtual void setTabletConfig(std::string const & tabletName, TabletConfig const & cfg);
    virtual warp::Interval<std::string> getTabletRange(std::string const & tabletName);
    virtual std::string getNewTabletFile(std::string const & tabletName);
    virtual std::string getNewLogFile();
    virtual std::pair<TablePtr, std::string> openTable(std::string const & uri);
};


#endif // KDI_TABLET_FILECONFIGMANAGER_H
