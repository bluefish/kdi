//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: kdi/meta/meta_util.h $
//
// Created 2008/03/19
//
// Copyright 2008 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef KDI_META_META_UTIL_H
#define KDI_META_META_UTIL_H

#include <kdi/table.h>
#include <warp/interval.h>
#include <string>

namespace kdi {
namespace meta {

    /// Get the meta row key for a tablet in the named table
    /// containing the given last row.
    std::string encodeMetaRow(strref_t tableName, strref_t row);
    
    /// Get the meta row key for the last tablet in the named table.
    std::string encodeLastMetaRow(strref_t tableName);

    /// Start a scan of the meta table location column.  The first
    /// cell in the scan will be the tablet that contains the given
    /// row, and the scan will terminate at the end of the named
    /// table.  If the named table doesn't exist in the meta table, an
    /// empty scan will be returned.
    CellStreamPtr metaLocationScan(
        TablePtr const & metaTable, strref_t tableName, strref_t row);

    /// Get the tablet upper row bound for the given meta row key.
    warp::IntervalPoint<std::string> getTabletRowBound(strref_t metaRow);

} // namespace meta
} // namespace kdi

#endif // KDI_META_META_UTIL_H
