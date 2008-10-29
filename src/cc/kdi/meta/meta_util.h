//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
// Created 2008-03-19
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

    /// Scan the META table cells for the given table name.
    CellStreamPtr metaScan(TablePtr const & metaTable, strref_t tableName);

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
