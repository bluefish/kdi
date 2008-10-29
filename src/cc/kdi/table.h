//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-09-06
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

#ifndef KDI_TABLE_H
#define KDI_TABLE_H

#include <kdi/strref.h>
#include <kdi/cell.h>
#include <flux/stream.h>
#include <boost/shared_ptr.hpp>
#include <string>

namespace kdi {

    /// Base table interface
    class Table;

    /// Pointer to a table instance
    typedef boost::shared_ptr<Table> TablePtr;

    /// Pointer to a const table instance
    typedef boost::shared_ptr<Table const> TableCPtr;

    // Forward declaration
    class ScanPredicate;

    // Forward declaration
    class RowInterval;

    // Stream typedefs
    typedef flux::Stream<RowInterval> RowIntervalStream;
    typedef boost::shared_ptr<RowIntervalStream> RowIntervalStreamPtr;

} // namespace kdi

//----------------------------------------------------------------------------
// Table
//----------------------------------------------------------------------------
class kdi::Table
{
public:
    virtual ~Table() {}

    // Cell-atomic interface.  Each cell is handled as an atomic (row,
    // column, timestamp, value) tuple.  This interface makes no
    // particular inter-cell operation ordering guarantees.

    /// Set a (row, column, timestamp, value) cell in the table.  If
    /// another cell with the same (row, column, timestamp) key
    /// already exists in the table, its value will be overwritten.
    virtual void set(strref_t row, strref_t column, int64_t timestamp,
                     strref_t value) = 0;

    /// Erase the cell with the given (row, column, timestamp) key.
    /// If the cell doesn't already exist, this operation has no
    /// effect.
    virtual void erase(strref_t row, strref_t column, int64_t timestamp) = 0;

    /// Insert a cell or cell erasure into the table.  The default
    /// implementation of this function calls either set() or erase().
    virtual void insert(Cell const & x);

    /// Scan over all cells in the table, visited in cell order.  No
    /// guarantee is made on the time-consistency of the cells
    /// returned in the scan.  If the table is modified after a scan
    /// begins, any of the modifications MAY appear in the scan, with
    /// no ordering guarantees.
    virtual CellStreamPtr scan() const = 0;

    /// Scan over a subset of the cells in the table, visited in cell
    /// order.  No guarantee is made on the time-consistency of the
    /// cells returned in the scan.  If the table is modified after a
    /// scan begins, any of the modifications MAY appear in the scan,
    /// with no ordering guarantees.  The default implementation of
    /// this function is simply a filter over the full table scan.
    /// Specific implementations may be smarter.
    virtual CellStreamPtr scan(ScanPredicate const & pred) const;

    /// Form a ScanPredicate from the given expression and use it to
    /// perform a selective scan, as above.  This is a convenience
    /// function to save typing.
    CellStreamPtr scan(strref_t predExpr) const;

    /// Block until all mutations on this table have been successfully
    /// committed.  In the event certain mutations have failed, this
    /// may throw an exception.
    virtual void sync() = 0;

    /// Get a sequence of roughly equal-sized row intervals in the
    /// table.  The size of each interval depends on the table
    /// implementation, but the intention is that they'll be fairly
    /// coarse, at a scale suitable for scheduling each interval as a
    /// separate job.  There will generally be more intervals for
    /// larger tables.  Not all implementations provide this facility.
    /// The default implementation returns a single interval
    /// containing all possible rows.
    virtual RowIntervalStreamPtr scanIntervals() const;

public:
    
    /// Open a table and return a handle to it.
    static TablePtr open(std::string const & uri);
};

#endif // KDI_TABLE_H
