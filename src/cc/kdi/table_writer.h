//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: kdi/table_writer.h $
//
// Created 2008/04/28
//
// Copyright 2008 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
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
