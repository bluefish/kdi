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

#include <kdi/tablet/TabletName.h>
#include <warp/strutil.h>
#include <warp/tuple_encode.h>
#include <ex/exception.h>

using namespace kdi::tablet;
using namespace warp;
using namespace ex;
using namespace std;

TabletName::TabletName(strref_t encodedName)
{
    string sep, last;
    decodeTuple(encodedName, tie(tableName, sep, last));

    if(sep == "\x01")
        lastRow = IntervalPoint<string>(last, PT_INCLUSIVE_UPPER_BOUND);
    else if(sep == "\x02")
        lastRow = IntervalPoint<string>(string(), PT_INFINITE_UPPER_BOUND);
    else
        raise<ValueError>("invalid encoded tablet name: %s",
                          reprString(encodedName));
}

TabletName::TabletName(strref_t tableName,
                       warp::IntervalPoint<std::string> lastRow) :
    tableName(tableName.begin(), tableName.end()),
    lastRow(lastRow)
{
    if(tableName.empty())
        raise<ValueError>("tableName cannot be empty");

    switch(lastRow.getType())
    {
        case PT_INFINITE_UPPER_BOUND:
        case PT_INCLUSIVE_UPPER_BOUND:
            break;

        default:
            raise<ValueError>("lastRow must be inclusive or infinite "
                              "upper bound");
    }
}

std::string TabletName::getEncoded() const
{
    if(lastRow.isFinite())
        return encodeTuple(make_tuple(tableName, "\x01", lastRow.getValue()));
    else
        return encodeTuple(make_tuple(tableName, "\x02", ""));
}
