//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-02-13
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

#include <kdi/sdstore_cell.h>
#include <kdi/sdstore_dynamic_cell.h>
#include <warp/util.h>
#include <warp/atomic.h>
#include <warp/timestamp.h>
#include <algorithm>

using namespace sdstore;
using namespace warp;
using namespace std;

//----------------------------------------------------------------------------
// CellInterpreter
//----------------------------------------------------------------------------
str_data_t CellInterpreter::getColumnFamily(void const * data) const
{
    str_data_t c = this->getColumn(data);
    char const * sep = find(c.begin(), c.end(), ':');
    return str_data_t(c.begin(), sep);
}

str_data_t CellInterpreter::getColumnQualifier(void const * data) const
{
    str_data_t c = this->getColumn(data);
    char const * sep = find_after(c.begin(), c.end(), ':');
    return str_data_t(sep, c.end());
}

bool CellInterpreter::isErasure(void const * data) const
{
    return false;
}

bool CellInterpreter::isLess(void const * data1, void const * data2) const
{
    if(int cmp = string_compare_3way(getRow(data1), getRow(data2)))
        return cmp < 0;
    else if(int cmp = string_compare_3way(getColumn(data1), getColumn(data2)))
        return cmp < 0;
    else
        return getTimestamp(data2) < getTimestamp(data1);
}

//----------------------------------------------------------------------------
// Cell
//----------------------------------------------------------------------------
std::ostream & sdstore::operator<<(std::ostream & o, Cell const & cell)
{
    if(!cell)
        return o << "(NULL)";

    str_data_t row = cell.getRow();
    str_data_t col = cell.getColumn();
    str_data_t val = cell.getValue();
    int64_t ts = cell.getTimestamp();

    o << '('
      << reprString(row.begin(), row.end(), true)
      << ','
      << reprString(col.begin(), col.end(), true)
      << ',';
    if(-10000 < ts && ts < 10000)
        o << '@' << ts;
    else
        o << Timestamp::fromMicroseconds(ts);
    o << ',';
    if(cell.isErasure())
        o << "ERASED";
    else
        o << reprString(val.begin(), val.end(), true);
    return o << ')';
}


//----------------------------------------------------------------------------
// makeCell
//----------------------------------------------------------------------------
Cell sdstore::makeCell(strref_t row, strref_t col, int64_t ts, strref_t val)
{
    return DynamicCell::make(row, col, ts, val);
}

//----------------------------------------------------------------------------
// makeCellErasure
//----------------------------------------------------------------------------
Cell sdstore::makeCellErasure(strref_t row, strref_t col, int64_t ts)
{
    return DynamicCellErasure::make(row, col, ts);
}
