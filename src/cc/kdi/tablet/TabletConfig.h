//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: kdi/tablet/TabletConfig.h $
//
// Created 2008/05/23
//
// Copyright 2008 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
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
