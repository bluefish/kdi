//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-09-19
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

#ifndef SDSTORE_DYNAMIC_CELL_H
#define SDSTORE_DYNAMIC_CELL_H

#include <kdi/sdstore_cell.h>
#include <warp/atomic.h>
#include <warp/string_range.h>
#include <boost/scoped_array.hpp>

namespace sdstore {

    /// A dynamically allocated Cell.
    class DynamicCell;

    /// A dynamically allocated Cell erasure.
    class DynamicCellErasure;

} // namespace sdstore


//----------------------------------------------------------------------------
// DynamicCell
//----------------------------------------------------------------------------
class sdstore::DynamicCell : private boost::noncopyable
{
    mutable warp::AtomicCounter refCount;
    boost::scoped_array<char> buf;
    char * colBegin;
    char * valBegin;
    char * end;
    int64_t timestamp;

    class Interpreter;

    DynamicCell(warp::strref_t row, warp::strref_t column,
                int64_t timestamp, warp::strref_t value) :
        buf(new char[row.size() + column.size() + value.size()]),
        colBegin(buf.get() + row.size()),
        valBegin(colBegin + column.size()),
        end(valBegin + value.size()),
        timestamp(timestamp)
    {
        memcpy(buf.get(), row.begin(), row.size());
        memcpy(colBegin, column.begin(), column.size());
        memcpy(valBegin, value.begin(), value.size());
    }

public:
    /// Make a dynamic Cell
    static Cell make(warp::strref_t row, warp::strref_t column,
                     int64_t timestamp, warp::strref_t value);

    size_t size() const
    {
        return sizeof(*this) + (end - buf.get());
    }
};


//----------------------------------------------------------------------------
// DynamicCellErasure
//----------------------------------------------------------------------------
class sdstore::DynamicCellErasure : private boost::noncopyable
{
    mutable warp::AtomicCounter refCount;
    boost::scoped_array<char> buf;
    char * colBegin;
    char * end;
    int64_t timestamp;

    class Interpreter;

    DynamicCellErasure(warp::strref_t row, warp::strref_t column,
                       int64_t timestamp) :
        buf(new char[row.size() + column.size()]),
        colBegin(buf.get() + row.size()),
        end(colBegin + column.size()),
        timestamp(timestamp)
    {
        memcpy(buf.get(), row.begin(), row.size());
        memcpy(colBegin, column.begin(), column.size());
    }

public:
    /// Make a dynamic Cell erasure
    static Cell make(warp::strref_t row, warp::strref_t column,
                     int64_t timestamp);

    size_t size() const
    {
        return sizeof(*this) + (end - buf.get());
    }
};


#endif // SDSTORE_DYNAMIC_CELL_H
