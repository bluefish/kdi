//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-09-19
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

#ifndef KDI_MEMORY_TABLE_H
#define KDI_MEMORY_TABLE_H

#include <kdi/table.h>
#include <kdi/cell.h>
#include <set>
#include <boost/shared_ptr.hpp>
#include <boost/enable_shared_from_this.hpp>
#include <boost/noncopyable.hpp>

namespace kdi {

    /// A Table implementation in main memory.  A scan will always
    /// include all modifications to the table that happened before
    /// the scan started.  It may also pick up modifications that
    /// happen after the start of the scan.
    class MemoryTable;

    /// Pointer to a MemoryTable
    typedef boost::shared_ptr<MemoryTable> MemoryTablePtr;

    /// Pointer to a const MemoryTable
    typedef boost::shared_ptr<MemoryTable const> MemoryTableCPtr;

} // namespace kdi

//----------------------------------------------------------------------------
// MemoryTable
//----------------------------------------------------------------------------
class kdi::MemoryTable
    : public kdi::Table,
      public boost::enable_shared_from_this<MemoryTable>,
      private boost::noncopyable

{
    struct Item
    {
        mutable Cell cell;
        Item(Cell const & cell) : cell(cell) {}
        bool operator<(Item const & o) const { return cell < o.cell; }
    };

    typedef std::set<Item> set_t;

    // The table maintains a current set of Cells.  Once a set node is
    // created it is never deleted, though its Cell value may be
    // swapped out for a different Cell with an identical key.  This
    // ensures that the set ordering invariant is maintained and set
    // iterators are never invalidated.
    set_t cells;

    // Approximate memory used by table, exclusive of *this
    size_t memUsage;

    // Should erasures be filtered?
    bool filterErasures;

    /// Insert a Cell into the set.
    void insert(Cell const & cell);

    /// Scan implementation for MemoryTable.
    class Scanner;

protected:
    // Constructor is protected -- use create()
    MemoryTable(bool filterErasures);

public:
    /// Create a new MemoryTable object, contained in a smart pointer
    static MemoryTablePtr create(bool filterErasures);

    virtual ~MemoryTable();

    virtual void set(strref_t row, strref_t column, int64_t timestamp,
                     strref_t value);

    virtual void erase(strref_t row, strref_t column, int64_t timestamp);

    virtual CellStreamPtr scan() const;
    virtual CellStreamPtr scan(ScanPredicate const & pred) const;

    virtual void sync() { /* nothing to do */ }

    /// Get the approximate memory used by Cells stored in this table.
    size_t getMemoryUsage() const;

    /// Get the number of Cells in the table.
    size_t getCellCount() const;

    CellStreamPtr scanWithErasures() const;
};


#endif // KDI_MEMORY_TABLE_H
