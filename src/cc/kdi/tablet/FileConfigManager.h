//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: kdi/tablet/FileConfigManager.h $
//
// Created 2008/05/21
//
// Copyright 2008 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
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
