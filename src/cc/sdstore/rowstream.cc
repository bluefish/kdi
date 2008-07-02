//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/sdstore/rowstream.cc#1 $
//
// Created 2007/02/14
//
// Copyright 2007 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#include "rowstream.h"
#include "rowbuffer.h"

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
