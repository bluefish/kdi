//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/kdi/table.h#2 $
//
// Created 2007/09/06
//
// Copyright 2007 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef KDI_TABLE_H
#define KDI_TABLE_H

#include <kdi/strref.h>
#include <kdi/cell.h>
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

public:
    
    /// Open a table and return a handle to it.
    static TablePtr open(std::string const & uri);
};

#endif // KDI_TABLE_H
