//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-02-14
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

#include <kdi/sdstore_rowbuffer.h>
#include <kdi/CellField.h>
#include <warp/strutil.h>
#include <algorithm>

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
        vector<Cell>::iterator i =
            std::lower_bound(buf.begin(), buf.end(), c);
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

StringRange RowBuffer::getRow() const
{
    if(empty())
        raise<RuntimeError>("getRow() on empty RowBuffer");

    return buf.back().getRow();
}

Cell const * RowBuffer::getCell(strref_t column) const
{
    vector<Cell>::const_iterator i =
        std::lower_bound(buf.begin(), buf.end(), kdi::CellColumn(column));

    if(i != end() && i->getColumn() == column)
        return &*i;
    else
        return 0;
}

StringRange RowBuffer::getValue(strref_t column) const
{
    Cell const * c = getCell(column);
    return c ? c->getValue() : StringRange();
}

ostream & sdstore::operator<<(ostream & o, RowBuffer const & row)
{
    StringRange rowName = row.getRow();
    o << '[' << reprString(rowName.begin(), rowName.end(), true);
    for(RowBuffer::cell_iterator ci = row.begin(); ci != row.end(); ++ci)
    {
        StringRange col = ci->getColumn();
        int64_t ts = ci->getTimestamp();
        StringRange val = ci->getValue();

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
