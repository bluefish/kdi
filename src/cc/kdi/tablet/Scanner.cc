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
    pred(pred),
    catchUp(false)
{
    log("Scanner %p: created", this);

    EX_CHECK_NULL(tablet);
    if(pred.getMaxHistory())
        raise<ValueError>("unsupported history predicate: %s", pred);

    // Open the scan
    reopen();
}

Scanner::~Scanner()
{
    log("Scanner %p: destroyed", this);
}

void Scanner::reopen()
{
    log("Scanner: reopening scan on tablet %s: %s", tablet->getName(), pred);

    // Synchronize with get() thread
    lock_t sync(mutex);

    // Clip our scan predicate to last seen cell
    ScanPredicate p;
    if(lastCell)
    {
        // We've already returned some cells.  We'll need to clip
        // to predicate and then play catchup on the next get()
        // call.
        p = pred.clipRows(makeLowerBound(str(lastCell.getRow())));
        catchUp = true;
    }
    else
    {
        // We haven't started yet -- just use normal predicate
        p = pred;
    }

    // Reopen cell stream
    cells = tablet->getMergedScan(p);
}
