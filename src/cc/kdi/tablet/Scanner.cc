//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
// Created 2008-05-14
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

#include <kdi/tablet/Scanner.h>
#include <kdi/tablet/Tablet.h>
#include <warp/log.h>
#include <ex/exception.h>

using namespace kdi;
using namespace kdi::tablet;
using namespace warp;
using namespace ex;
using namespace std;

//----------------------------------------------------------------------------
// Scanner
//----------------------------------------------------------------------------
Scanner::Scanner(TabletCPtr const & tablet, ScanPredicate const & pred) :
    tablet(tablet),
    pred(pred)
{
    //log("Scanner %p: created", this);

    EX_CHECK_NULL(tablet);
    if(pred.getMaxHistory())
        raise<ValueError>("unsupported history predicate: %s", pred);
}

Scanner::~Scanner()
{
    //log("Scanner %p: destroyed", this);
}

bool Scanner::get(Cell & x)
{
    // Synchronize with reopen() thread
    lock_t sync(mutex);

    // Check to see if we need to open the scanner
    if(!cells)
    {
        log("Scanner: reopening scan on table %s: %s", tablet->getPrettyName(), pred);

        // If we have a last cell, we need to clip our scan predicate
        // to last row, then fast forward the scan to the last cell.
        if(lastCell)
        {
            // Reopen the scan using a clipped predicate.
            cells = tablet->getMergedScan(
                pred.clipRows(
                    makeLowerBound(lastCell.getRow().toString())));

            // Fast forward the scan.  Keep reading cells until we
            // find one after the last cell we returned.  Return that
            // one.
            while(cells->get(x))
            {
                if(lastCell < x)
                {
                    // Found a cell after the last cell.  Remember it
                    // and return.
                    lastCell = x;
                    return true;
                }
            }

            // There were no cells after our last cell.  This is the
            // end of the stream.
            return false;
        }

        // Otherwise we haven't returned any cells yet.  Use normal
        // predicate and open the scan.
        cells = tablet->getMergedScan(pred);
    }

    // Continue with normal scan
    if(!cells->get(x))
    {
        // No more cells: end of stream
        return false;
    }

    // Remember last cell so we can reopen if necessary
    lastCell = x;
    return true;
}

void Scanner::reopen()
{
    // Release scanner -- we'll reopen it on the next call to get()
    lock_t sync(mutex);
    cells.reset();
}
