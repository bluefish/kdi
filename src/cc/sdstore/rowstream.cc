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

#include <sdstore/rowstream.h>
#include <sdstore/rowbuffer.h>

using namespace sdstore;
using namespace ex;

RowStream::RowStream(CellStreamHandle const & cellStream) :
    input(cellStream)
{
    if(!cellStream)
        raise<ValueError>("null cellStream");
}

bool RowStream::fetch()
{
    // Finish current row
    Cell x;
    while(rowCell)
        get(x);

    // Try start another row
    if(pending)
    {
        // Have a cell pending from last get()
        rowCell = pending;
        pending.release();
    }
    else if(!input || !input->get(rowCell))
    {
        // No cells left
        return false;
    }

    // Ready to scan new row
    firstInRow = true;
    rowName = rowCell.getRow();
    return true;
}

void RowStream::pipeFrom(CellStreamHandle const & input)
{
    rowCell.release();
    pending.release();
    this->input = input;
}
    
bool RowStream::get(RowBuffer & buf)
{
    buf.clear();
    if(fetch())
    {
        Cell x;
        while(get(x))
            buf.appendNoCheck(x);
    }
    return !buf.empty();
}
