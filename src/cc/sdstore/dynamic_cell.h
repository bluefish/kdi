//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/sdstore/dynamic_cell.h#1 $
//
// Created 2007/09/19
//
// Copyright 2007 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef SDSTORE_DYNAMIC_CELL_H
#define SDSTORE_DYNAMIC_CELL_H

#include "cell.h"
#include "warp/atomic.h"
#include "warp/strref.h"
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
