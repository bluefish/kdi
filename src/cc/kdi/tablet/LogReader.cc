//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: kdi/tablet/LogReader.cc $
//
// Created 2008/05/27
//
// Copyright 2008 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#include <kdi/tablet/LogReader.h>
#include <kdi/tablet/LogEntry.h>

using namespace kdi;
using namespace kdi::tablet;
using namespace std;

namespace
{
    bool operator==(warp::StringData const & a, std::string const & b)
    {
        return a.size() == b.size() &&
            std::equal(a.begin(), a.end(), b.c_str());
    }

    bool operator!=(warp::StringData const & a, std::string const & b)
    {
        return !(a == b);
    }
}

//----------------------------------------------------------------------------
// LogReader
//----------------------------------------------------------------------------
LogReader::LogReader(string const & logFn, string const & tabletName) :
    input(oort::inputStream(logFn)),
    tabletName(tabletName),
    nextCell(0),
    endCell(0)
{
}

bool LogReader::get(Cell & x)
{
    // Make sure we have a cell to return
    while(nextCell == endCell)
    {
        // Get the next log entry in the file
        if(!input->get(logEntry))
            // No more log, no more cells
            return false;

        // Skip the entry if it is for a different tablet
        LogEntry const * ent = logEntry.as<LogEntry>();
        if(*ent->tabletName != tabletName)
            continue;

        // Update cell pointers to new entry's cell block
        nextCell = ent->cells.begin();
        endCell = ent->cells.end();
    }

    // We have now have nextCell != endCell.  Return a cell.
    if(nextCell->value)
    {
        // It's a regular cell
        x = makeCell(nextCell->key.row,
                     nextCell->key.column,
                     nextCell->key.timestamp,
                     nextCell->value);
    }
    else
    {
        // It's an erasure
        x = makeCellErasure(nextCell->key.row,
                            nextCell->key.column,
                            nextCell->key.timestamp);
    }

    // Advance cell pointer and return
    ++nextCell;
    return true;
}
