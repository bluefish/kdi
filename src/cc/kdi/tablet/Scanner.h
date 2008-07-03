//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
// Created 2008-05-14
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

#ifndef KDI_TABLET_SCANNER_H
#define KDI_TABLET_SCANNER_H

#include <kdi/tablet/forward.h>
#include <kdi/cell.h>
#include <kdi/scan_predicate.h>
#include <boost/shared_ptr.hpp>
#include <boost/thread/mutex.hpp>

namespace kdi {
namespace tablet {

    class Scanner;

} // namespace tablet
} // namespace kdi

//----------------------------------------------------------------------------
// Scanner
//----------------------------------------------------------------------------
class kdi::tablet::Scanner
    : public kdi::CellStream
{
    TabletCPtr tablet;
    ScanPredicate pred;
    
    CellStreamPtr cells;
    Cell lastCell;
    bool catchUp;

    // We need locking to coordinate the get() thread and the reopen()
    // thread.  It would be nice if the get() function didn't need to
    // grab the lock every time.  Perhaps this synchronization can be
    // moved out later.
    typedef boost::mutex::scoped_lock lock_t;
    boost::mutex mutex;

public:
    Scanner(TabletCPtr const & tablet, ScanPredicate const & pred);
    ~Scanner();

    bool get(Cell & x)
    {
        // Synchronize with reopen() thread
        lock_t sync(mutex);

        // Are we in catchUp mode?  This happens after reopening the
        // cell stream.
        if(catchUp)
        {
            // Catch up mode.  Keep reading cells until we find one
            // after the last cell we returned.  Return that one.
            catchUp = false;
            while(cells->get(x))
            {
                if(lastCell < x)
                {
                    // Found the next cell.  Remember it and return.
                    lastCell = x;
                    return true;
                }
            }

            // There were no cells after our last cell.  This is the
            // end of the stream.
            return false;
        }
        // Normal mode
        else if(cells->get(x))
        {
            // Remember and return the next cell
            lastCell = x;
            return true;
        }
        else
        {
            // No more cells: end of stream
            return false;
        }
    }

    /// Reopen the scan from our Tablet.  This can be called when the
    /// Tablet's table set has changed.  The scan will resume after
    /// the last cell seen in the current scan.  New data may be
    /// included in the updated scan.
    void reopen();
};

#endif // KDI_TABLET_SCANNER_H
