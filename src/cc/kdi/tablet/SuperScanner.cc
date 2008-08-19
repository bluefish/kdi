//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
// Created 2008-08-11
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

#include <kdi/tablet/SuperScanner.h>
#include <kdi/tablet/SuperTablet.h>
#include <kdi/tablet/Tablet.h>

using namespace kdi;
using namespace kdi::tablet;
using namespace warp;
using namespace std;


//----------------------------------------------------------------------------
// SuperScanner
//----------------------------------------------------------------------------
SuperScanner::SuperScanner(SuperTabletCPtr const & superTablet,
                           ScanPredicate const & pred) :
    superTablet(superTablet),
    pred(pred)
{
    if(pred.getRowPredicate())
        remainingRows = *pred.getRowPredicate();
    else
        remainingRows.add(Interval<string>().setInfinite());
}

bool SuperScanner::get(Cell & x)
{
    boost::mutex::scoped_lock lock(mutex);

    // Reopen scanner if we need to
    if(!scanner)
    {
        // Set min row from last cell if we have one
        if(lastCell)
        {
            setMinRow(
                IntervalPoint<string>(
                    lastCell.getRow().toString(),
                    PT_INCLUSIVE_LOWER_BOUND
                    )
                );
        }

        // Anything left?
        if(remainingRows.isEmpty())
            return false;

        // Reopen scanner
        TabletPtr tablet = superTablet->getTablet(*remainingRows.begin());
        currentRows = tablet->getRows();
        scanner = tablet->scan(pred.clipRows(currentRows));

        // Advance scan until it is past our last seen cell
        if(lastCell)
        {
            while(scanner->get(x))
            {
                if(lastCell < x)
                {
                    lastCell = x;
                    return true;
                }
            }
        }
    }

    for(;;)
    {
        // If we can get a cell, return it
        if(scanner->get(x))
        {
            lastCell = x;
            return true;
        }

        // Out of cells in current scanner, try to open next
        // scanner
        if(!openNextScanner())
            return false;
    }
}

void SuperScanner::reopen()
{
    boost::mutex::scoped_lock lock(mutex);

    if(!scanner)
        return;

    // Release scanner -- we'll reopen it on the next call to get()
    scanner.reset();
}

void SuperScanner::setMinRow(IntervalPoint<string> const & minRow)
{
    // Clip what what we've already scanned from what's left
    remainingRows.clip(
        Interval<string>(
            minRow,
            IntervalPoint<string>(string(), PT_INFINITE_UPPER_BOUND)
            )
        );

    // Update scan predicate
    pred.setRowPredicate(remainingRows);
}

bool SuperScanner::openNextScanner()
{
    // Did the last set go to the end?
    if(currentRows.getUpperBound().isInfinite())
        return false;

    // Clip what what we've already scanned from what's left
    setMinRow(currentRows.getUpperBound().getAdjacentComplement());

    // Anything left in the predicate?
    if(remainingRows.isEmpty())
        return false;

    // Get the next tablet
    TabletPtr tablet = superTablet->getTablet(*remainingRows.begin());
    currentRows = tablet->getRows();
    scanner = tablet->scan(pred.clipRows(currentRows));

    return true;
}
