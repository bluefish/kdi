//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
// Created 2008-07-17
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

#ifndef KDI_TABLET_TABLETNAME_H
#define KDI_TABLET_TABLETNAME_H

#include <string>
#include <warp/string_range.h>
#include <warp/interval.h>

namespace kdi {
namespace tablet {

    class TabletName
    {
        std::string tableName;
        warp::IntervalPoint<std::string> lastRow;
        
    public:
        /// Reconstruct a tablet name from an encoded string.
        explicit TabletName(warp::strref_t encodedName);

        /// Construct a tablet name from a table name and a row upper
        /// bound.
        TabletName(warp::strref_t tableName,
                   warp::IntervalPoint<std::string> lastRow);

        /// Get the tablet name as a single encoded string.
        std::string getEncoded() const;

        /// Get the table name for the tablet.
        std::string const & getTableName() const
        {
            return tableName;
        }

        /// Get the last row bound for the tablet.
        warp::IntervalPoint<std::string> const & getLastRow() const
        {
            return lastRow;
        }
    };

} // namespace tablet
} // namespace kdi

#endif // KDI_TABLET_TABLETNAME_H
