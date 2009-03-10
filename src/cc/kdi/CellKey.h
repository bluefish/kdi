//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-02-27
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

#ifndef KDI_CELLKEY_H
#define KDI_CELLKEY_H

#include <warp/string_range.h>
#include <kdi/strref.h>
#include <warp/util.h>
#include <string>

namespace kdi {

    class CellKeyLt;
    class CellKey;
    class CellKeyRef;

} // namespace kdi

//----------------------------------------------------------------------------
// CellKeyLt
//----------------------------------------------------------------------------
class kdi::CellKeyLt
{
public:
    template <class A, class B>
    bool operator()(A const & a, B const & b) const
    {
        if(int c = warp::string_compare(getCellRow(a), getCellRow(b)))
            return c < 0;
        else if(int c = warp::string_compare(getCellColumn(a), getCellColumn(b)))
            return c < 0;
        else
            return getCellTimestamp(b) < getCellTimestamp(a);
    }
};

//----------------------------------------------------------------------------
// CellKey
//----------------------------------------------------------------------------
class kdi::CellKey
{
    std::string row;
    std::string column;
    int64_t timestamp;

public:
    void setRow(strref_t x)       { row.assign(x.begin(), x.end()); }
    void setColumn(strref_t x)    { column.assign(x.begin(), x.end()); }
    void setTimestamp(int64_t x)  { timestamp = x; }

    std::string const & getRow() const     { return row; }
    std::string const & getColumn() const  { return column; }
    int64_t getTimestamp() const           { return timestamp; }

    template <class T>
    bool operator<(T const & o) const
    {
        return CellKeyLt()(*this,o);
    }
};

namespace kdi {

    inline std::string const & getCellRow(CellKey const & x)
    {
        return x.getRow();
    }

    inline std::string const & getCellColumn(CellKey const & x)
    {
        return x.getColumn();
    }

    inline int64_t getCellTimestamp(CellKey const & x)
    {
        return x.getTimestamp();
    }

}

//----------------------------------------------------------------------------
// CellKeyRef
//----------------------------------------------------------------------------
class kdi::CellKeyRef
{
    warp::StringRange row;
    warp::StringRange column;
    int64_t timestamp;

public:
    void setRow(strref_t x)       { row = x; }
    void setColumn(strref_t x)    { column = x; }
    void setTimestamp(int64_t x)  { timestamp = x; }

    strref_t getRow() const       { return row; }
    strref_t getColumn() const    { return column; }
    int64_t getTimestamp() const  { return timestamp; }

    template <class T>
    bool operator<(T const & o) const
    {
        return CellKeyLt()(*this,o);
    }
};

namespace kdi {

    inline strref_t getCellRow(CellKeyRef const & x)
    {
        return x.getRow();
    }

    inline strref_t getCellColumn(CellKeyRef const & x)
    {
        return x.getColumn();
    }

    inline int64_t getCellTimestamp(CellKeyRef const & x)
    {
        return x.getTimestamp();
    }

}

#endif // KDI_CELLKEY_H
