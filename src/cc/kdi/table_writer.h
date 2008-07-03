//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
// Created 2008-04-28
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

#ifndef KDI_TABLE_WRITER_H
#define KDI_TABLE_WRITER_H

#include <kdi/table.h>
#include <kdi/cell.h>
#include <kdi/row_buffer.h>

namespace kdi {
    
    /// Writes a stream of Cells to a Table.
    class TableWriter;

} // namespace kdi

//----------------------------------------------------------------------------
// TableWriter
//----------------------------------------------------------------------------
class kdi::TableWriter
    : virtual public CellStream,
      virtual public RowBufferStream
{
    TablePtr table;

public:
    explicit TableWriter(TablePtr const & table) :
        table(table)
    {
        EX_CHECK_NULL(table);
    }

    void put(Cell const & x)
    {
        if(!x.isErasure())
            table->set(x.getRow(), x.getColumn(), x.getTimestamp(),
                       x.getValue());
        else
            table->erase(x.getRow(), x.getColumn(), x.getTimestamp());
    }

    void put(RowBuffer const & x)
    {
        for(RowBuffer::cell_iterator it = x.begin(); it != x.end(); ++it)
            TableWriter::put(*it);
    }

    void flush()
    {
        table->sync();
    }
};


#endif // KDI_TABLE_WRITER_H
