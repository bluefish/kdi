//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: kdi/CellKey.h $
//
// Created 2009/02/27
//
// Copyright 2009 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef KDI_CELLKEY_H
#define KDI_CELLKEY_H

#include <kdi/strref.h>
#include <warp/util.h>
#include <string>

namespace kdi {

    class CellKey;

} // namespace kdi

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

    bool operator<(CellKey const & o) const
    {
        return warp::order(
            row, o.row,
            column, o.column,
            o.timestamp, timestamp);
    }
};


#endif // KDI_CELLKEY_H
