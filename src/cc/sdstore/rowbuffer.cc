//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-02-14
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

#include "rowbuffer.h"
#include "warp/strutil.h"

using namespace sdstore;
using namespace warp;
using namespace ex;
using namespace std;

bool RowBuffer::append(Cell const & c)
{
    if(buf.empty())
    {
        // Can always add to an empty buffer
        buf.push_back(c);
        return true;
    }
    else if(!(buf.back() < c))
    {
        // Cell is out of order
        raise<ValueError>("cell out of order: last=%s new=%s",
                          buf.back(), c);
    }
    else if(getRow() == c.getRow())
    {
        // Cell is in right row -- add it
        buf.push_back(c);
        return true;
    }
    else
    {
        // Cell is in a later row
        return false;
    }
}

bool RowBuffer::insert(Cell const & c)
{
    if(buf.empty())
    {
        // Can always add to an empty buffer
        buf.push_back(c);
        return true;
    }
    else if(getRow() == c.getRow())
    {
        // Cell is in right row -- insert it
        vector<Cell>::iterator i = lower_bound(buf.begin(), buf.end(), c);
        if(i != buf.end() && *i == c)
        {
            // Already exists: overwrite
            *i = c;
        }
        else
        {
            // Insert in order
            buf.insert(i, c);
        }
        return true;
    }
    else
    {
        // Cell is in the wrong row
        return false;
    }
}

str_data_t RowBuffer::getRow() const
{
    if(empty())
        raise<RuntimeError>("getRow() on empty RowBuffer");

    return buf.back().getRow();
}

Cell const * RowBuffer::getCell(strref_t column) const
{
    if(empty())
        return 0;

    Cell const * lo = &*begin();
    Cell const * hi = lo + size();
    while(lo < hi)
    {
        Cell const * mid = lo + (hi-lo) / 2;
        str_data_t midColumn = mid->getColumn();
        if(column < midColumn)
            hi = mid;
        else if(column == midColumn)
            return mid;
        else
            lo = mid + 1;
    }
    return 0;
}

str_data_t RowBuffer::getValue(strref_t column) const
{
    Cell const * c = getCell(column);
    return c ? c->getValue() : str_data_t();
}

ostream & sdstore::operator<<(ostream & o, RowBuffer const & row)
{
    str_data_t rowName = row.getRow();
    o << '[' << reprString(rowName.begin(), rowName.end(), true);
    for(RowBuffer::cell_iterator ci = row.begin(); ci != row.end(); ++ci)
    {
        str_data_t col = ci->getColumn();
        int64_t ts = ci->getTimestamp();
        str_data_t val = ci->getValue();

        o << ",\n   ("
          << reprString(col.begin(), col.end(), true)
          << ','
          << ts
          << ','
          << reprString(val.begin(), val.end(), true)
          << ')';
    }
    o << ']';
    return o;
}
