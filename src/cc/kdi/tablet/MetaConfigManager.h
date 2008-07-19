//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: kdi/tablet/MetaConfigManager.h $
//
// Created 2008/07/14
//
// Copyright 2008 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef KDI_TABLET_METACONFIGMANAGER_H
#define KDI_TABLET_METACONFIGMANAGER_H

#include <kdi/tablet/ConfigManager.h>
#include <boost/noncopyable.hpp>

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
      private boost::noncopyable
{
    TablePtr metaTable;
    std::string rootDir;

public:
    explicit MetaConfigManager(std::string const & rootDir);
    ~MetaConfigManager();

    // ConfigManager API
    virtual TabletConfig getTabletConfig(std::string const & tabletName);
    virtual void setTabletConfig(std::string const & tabletName, TabletConfig const & cfg);
    virtual std::string getNewTabletFile(std::string const & tabletName);
    virtual std::string getNewLogFile();
    virtual std::pair<TablePtr, std::string> openTable(std::string const & uri);

    void loadFixedTable(std::string const & tableName);
    void loadMeta(std::string const & metaTableUri);
};



#endif // KDI_TABLET_METACONFIGMANAGER_H
