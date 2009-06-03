//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-03-18
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

#ifndef KDI_SERVER_NAME_UTIL_H
#define KDI_SERVER_NAME_UTIL_H

#include <kdi/server/errors.h>
#include <kdi/strref.h>
#include <warp/string_range.h>
#include <warp/functional.h>
#include <warp/interval.h>
#include <string>
#include <algorithm>

namespace kdi {
namespace server {

    /// Return true iff the character is valid for a table name.
    inline bool isTableChar(char c)
    {
        return ( ('a' <= c && c <= 'z') ||
                 ('A' <= c && c <= 'Z') ||
                 ('0' <= c && c <= '9') ||
                 c == '/' ||
                 c == '-' ||
                 c == '_' );
    }

    /// Functor to wrap isTableChar
    struct IsTableChar
    {
        inline bool operator()(char c) const
        {
            return isTableChar(c);
        }
    };

    /// Parse a tablet name and extract the table name
    inline warp::StringRange extractTableName(strref_t tabletName)
    {
        char const * p = std::find_if(
            tabletName.begin(), tabletName.end(),
            warp::logical_not<IsTableChar>());

        if(p == tabletName.begin() ||
           p == tabletName.end())
        {
            throw BadTabletNameError();
        }

        switch(*p)
        {
            case ' ':
                break;

            case '!':
                if(p+1 != tabletName.end())
                    throw BadTabletNameError();
                break;

            default:
                throw BadTabletNameError();
        }

        return warp::StringRange(tabletName.begin(), p);
    }

    /// Decode a tablet name into a table name and a row upper bound
    inline void decodeTabletName(
        strref_t tabletName,
        std::string & tableName,
        warp::IntervalPoint<std::string> & lastRow)
    {
        strref_t t = extractTableName(tabletName);
        t.assignTo(tableName);
        if(*t.end() == '!')
            lastRow = warp::IntervalPoint<std::string>(
                std::string(),
                warp::PT_INFINITE_UPPER_BOUND);
        else
            lastRow = warp::IntervalPoint<std::string>(
                std::string(t.end()+1, tabletName.end()),
                warp::PT_INCLUSIVE_UPPER_BOUND);
    }

    /// Encode a table name and row upper bound into a tablet name
    inline void encodeTabletName(
        strref_t tableName,
        warp::IntervalPoint<std::string> const & lastRow,
        std::string & tabletName)
    {
        if(!tableName ||
           std::find_if(
               tableName.begin(),
               tableName.end(),
               warp::logical_not<IsTableChar>()) != tableName.end())
        {
            throw BadTabletNameError();
        }

        tableName.assignTo(tabletName);
        switch(lastRow.getType())
        {
            case warp::PT_INFINITE_UPPER_BOUND:
                tabletName += '!';
                break;
                
            case warp::PT_INCLUSIVE_UPPER_BOUND:
                tabletName += ' ';
                tabletName += lastRow.getValue();
                break;

            default:
                throw BadTabletNameError();
        }
    }

    inline std::string encodeTabletName(
        strref_t tableName,
        warp::IntervalPoint<std::string> const & lastRow)
    {
        std::string r;
        encodeTabletName(tableName, lastRow, r);
        return r;
    }

} // namespace server
} // namespace kdi

#endif // KDI_SERVER_NAME_UTIL_H
