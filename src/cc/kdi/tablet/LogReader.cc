//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
// Created 2008-05-27
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
        x = makeCell(*nextCell->key.row,
                     *nextCell->key.column,
                     nextCell->key.timestamp,
                     *nextCell->value);
    }
    else
    {
        // It's an erasure
        x = makeCellErasure(*nextCell->key.row,
                            *nextCell->key.column,
                            nextCell->key.timestamp);
    }

    // Advance cell pointer and return
    ++nextCell;
    return true;
}
