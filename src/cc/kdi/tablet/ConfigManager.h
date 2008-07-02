//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: kdi/tablet/ConfigManager.h $
//
// Created 2008/05/17
//
// Copyright 2008 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef KDI_TABLET_CONFIGMANAGER_H
#define KDI_TABLET_CONFIGMANAGER_H

#include <kdi/tablet/forward.h>
#include <warp/interval.h>
#include <warp/config.h>
#include <string>

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

    /// Load the config for the named Tablet.
    virtual TabletConfig getTabletConfig(std::string const & tabletName) = 0;

    /// Save the config for the named Tablet.
    virtual void setTabletConfig(std::string const & tabletName, TabletConfig const & cfg) = 0;

    /// Get the row range the named Tablet should serve.
    virtual warp::Interval<std::string> getTabletRange(std::string const & tabletName) = 0;

    /// Get a unique path for a new tablet file.  The directory
    /// containing the returned file should already exist.  If the
    /// returned file exists, it will be overwritten.
    virtual std::string getNewTabletFile(std::string const & tabletName) = 0;

    /// Get a unique path for a new shared log file.  The directory
    /// containing the returned file should already exist.  If the
    /// returned file exists, it will be overwritten.
    virtual std::string getNewLogFile() = 0;

    /// Open a table URI.  The ConfigManager returns the table and a
    /// URI that can be used to reopen the table.
    virtual std::pair<TablePtr, std::string> openTable(std::string const & uri) = 0;
};

#endif // KDI_TABLET_CONFIGMANAGER_H
