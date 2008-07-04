//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-02-14
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

#ifndef SDSTORE_ROWBUFFER_H
#define SDSTORE_ROWBUFFER_H

#include <kdi/sdstore_cell.h>
#include <warp/strref.h>
#include <vector>
#include <iostream>

namespace sdstore
{
    class RowBuffer;

    std::ostream & operator<<(std::ostream & o, RowBuffer const & row);
}

//----------------------------------------------------------------------------
// RowBuffer
//----------------------------------------------------------------------------
class sdstore::RowBuffer
{
public:
    typedef std::vector<Cell>::const_iterator cell_iterator;

private:
    std::vector<Cell> buf;

public:
    /// Clear row contents.
    void clear() { buf.clear(); }

    /// Get the number of cells in the row.
    size_t size() const { return buf.size(); }

    /// Check if row is empty.
    bool empty() const { return buf.empty(); }

    /// Convert to true iff non-empty.
    operator bool() const { return !empty(); }

    /// Get start iterator over cells in row.
    cell_iterator begin() const { return buf.begin(); }
        
    // Get end iterator over cells in row.
    cell_iterator end() const { return buf.end(); }

    /// Try to add a cell to the current row.  This will only
    /// succeed if the cell belongs to the same row as other cells
    /// already in the buffer (if any).  Cells must be added in
    /// increasing order.  Use insert for out-of-order cells.
    /// @return true iff the cell was added
    bool append(Cell const & c);

    /// Append the cell to the current row.  Don't do any checking.
    /// The caller must ensure that the cell belongs in the row and is
    /// in the correct order.
    void appendNoCheck(Cell const & c) { buf.push_back(c); }

    /// Try to add a cell to the current row.  This will only
    /// succeed if the cell belongs to the same row as other cells
    /// already in the buffer (if any).  Cells may be added in any
    /// order, but duplicates will be overwritten.
    /// @return true iff the cell was added
    bool insert(Cell const & c);

    /// Get the current row name for this buffer.  Row must not be
    /// empty.
    warp::str_data_t getRow() const;

    /// Get a Cell matching the given column name.  If the no such
    /// Cell exists, return null.
    Cell const * getCell(warp::strref_t column) const;

    /// Get the cell value associated with the given column.  A
    /// missing cell is indistinguishable from an empty cell, as both
    /// will return an empty string.
    warp::str_data_t getValue(warp::strref_t column) const;
};

#endif // SDSTORE_ROWBUFFER_H
