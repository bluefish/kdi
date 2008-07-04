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

#ifndef SDSTORE_ROWSTREAM_H
#define SDSTORE_ROWSTREAM_H

#include <kdi/sdstore_cell.h>
#include <flux/stream.h>
#include <string.h>

namespace sdstore
{
    class RowStream;

    // Forward declaration
    class RowBuffer;
}


//----------------------------------------------------------------------------
// RowStream
//----------------------------------------------------------------------------
class sdstore::RowStream : public CellStream
{
    CellStreamHandle input;
    Cell rowCell;
    Cell pending;
    warp::str_data_t rowName;
    bool firstInRow;

    inline bool isSameRow(warp::str_data_t const & o)
    {
        return (rowName.size() == o.size() &&
                (rowName.begin() == o.begin() ||
                 memcmp(rowName.begin(), o.begin(), rowName.size()) == 0));
    }

public:
    RowStream() {}
    explicit RowStream(CellStreamHandle const & cellStream);

    /// Get all cells in row in a single buffer
    bool get(RowBuffer & buf);

    // CellStream interface
    virtual bool get(Cell & x)
    {
        if(!rowCell)
        {
            // Not currently in a row
            return false;
        }
        else if(firstInRow)
        {
            // This is the first get in the row
            firstInRow = false;
            x = rowCell;
            return true;
        }
        else if(!input->get(x))
        {
            // Was not able to get another cell
            rowCell.release();
            return false;
        }
        else if(!isSameRow(x.getRow()))
        {
            // Got a cell, but it is not in this row
            pending = x;
            x.release();
            rowCell.release();
            return false;
        }
        else
        {
            // Got a good cell in this row
            return true;
        }
    }
    virtual bool fetch();
    virtual void pipeFrom(CellStreamHandle const & input);
};

#endif // SDSTORE_ROWSTREAM_H
