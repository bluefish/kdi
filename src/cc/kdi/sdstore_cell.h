//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-02-12
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

#ifndef SDSTORE_CELL_H
#define SDSTORE_CELL_H

#include <warp/util.h>
#include <warp/strutil.h>
#include <warp/strref.h>
#include <flux/stream.h>
#include <stdint.h>
#include <assert.h>
#include <iostream>

namespace sdstore
{
    /// Class to interpret different kinds of Cell data.  Essentially
    /// acts as a vtable without binding the vtable pointer in the
    /// actual data.
    class CellInterpreter;

    /// Uniform Cell value interface over various backend data
    /// provider implementations.
    class Cell;

    typedef flux::Stream<Cell> CellStream;
    typedef CellStream::handle_t CellStreamHandle;

    /// Make a dynamically-allocated Cell
    Cell makeCell(warp::strref_t row, warp::strref_t column,
                  int64_t timestamp, warp::strref_t value);

    /// Make a dynamically-allocated Cell erasure
    Cell makeCellErasure(warp::strref_t row, warp::strref_t column,
                         int64_t timestamp);
}

//----------------------------------------------------------------------------
// CellInterpreter
//----------------------------------------------------------------------------
class sdstore::CellInterpreter
{
public:
    virtual ~CellInterpreter() {}

    virtual warp::str_data_t getRow(void const * data) const = 0;
    virtual warp::str_data_t getColumn(void const * data) const = 0;
    virtual warp::str_data_t getValue(void const * data) const = 0;
    virtual int64_t getTimestamp(void const * data) const = 0;

    virtual warp::str_data_t getColumnFamily(void const * data) const;
    virtual warp::str_data_t getColumnQualifier(void const * data) const;

    virtual bool isErasure(void const * data) const;

    virtual bool isLess(void const * data1, void const * data2) const;
    
    virtual void addRef(void const * data) const = 0;
    virtual void release(void const * data) const = 0;
};


//----------------------------------------------------------------------------
// Cell
//----------------------------------------------------------------------------
class sdstore::Cell
{
    CellInterpreter const * interp;
    void const * data;

    struct unspecified_bool_t {};

public:
    Cell() :
        interp(0)
    {
    }

    Cell(Cell const & o) :
        interp(o.interp), data(o.data)
    {
        if(interp)
            interp->addRef(data);
    }

    Cell(CellInterpreter const * interp, void const * data) :
        interp(interp), data(data)
    {
        if(interp)
            interp->addRef(data);
    }

    Cell const & operator=(Cell const & o)
    {
        if(&o != this)
        {
            if(interp)
                interp->release(data);
            interp = o.interp;
            data = o.data;
            if(interp)
                interp->addRef(data);
        }
        return *this;
    }

    ~Cell()
    {
        if(interp)
            interp->release(data);
    }

    bool isNull() const { return interp == 0; }
    operator unspecified_bool_t const *() const {
        return reinterpret_cast<unspecified_bool_t const *>(interp);
    }

    /// Get row name of cell
    warp::str_data_t getRow() const
    {
        assert(!isNull());
        return interp->getRow(data);
    }

    /// Get column name of cell
    warp::str_data_t getColumn() const
    {
        assert(!isNull());
        return interp->getColumn(data);
    }

    /// Get column family of cell
    warp::str_data_t getColumnFamily() const
    {
        assert(!isNull());
        return interp->getColumnFamily(data);
    }

    /// Get column qualifier of cell
    warp::str_data_t getColumnQualifier() const
    {
        assert(!isNull());
        return interp->getColumnQualifier(data);
    }

    /// Get value of cell
    warp::str_data_t getValue() const
    {
        assert(!isNull());
        return interp->getValue(data);
    }

    /// Get value of cell cast to a type
    template <class T>
    T const & getValueAs() const
    {
        assert(!isNull());
        warp::str_data_t v(getValue());
        if(v.size() < sizeof(T))
        {
            using namespace ex;
            raise<RuntimeError>
                ("cell value is smaller than result size of %d: %s",
                 sizeof(T), warp::reprString(v.begin(), v.end(), true));
        }
        return *reinterpret_cast<T const *>(v.begin());
    }

    /// Get timestamp of cell
    int64_t getTimestamp() const
    {
        assert(!isNull());
        return interp->getTimestamp(data);
    }
    
    /// Return true iff this Cell is an erasure for the given (row,
    /// column, timestamp) key.
    bool isErasure() const
    {
        assert(!isNull());
        return interp->isErasure(data);
    }

    /// Release cell data.  Makes this cell null
    void release()
    {
        if(interp)
        {
            interp->release(data);
            interp = 0;
        }
    }

    bool operator==(Cell const & o) const
    {
        return ( getRow() == o.getRow() &&
                 getColumn() == o.getColumn() &&
                 getTimestamp() == o.getTimestamp() );
    }

    bool operator<(Cell const & o) const
    {
        assert(!isNull());
        if(interp == o.interp)
        {
            return interp->isLess(data, o.data);
        }
        else
        {
            using warp::string_compare_3way;
            if(int cmp = string_compare_3way(getRow(), o.getRow()))
                return cmp < 0;
            else if(int cmp = string_compare_3way(getColumn(), o.getColumn()))
                return cmp < 0;
            else
                return o.getTimestamp() < getTimestamp();
        }
    }
};

namespace sdstore
{
    std::ostream & operator<<(std::ostream & o, Cell const & cell);
}


#endif // SDSTORE_CELL_H
