//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: kdi/tablet/Scanner.cc $
//
// Created 2008/05/14
//
// Copyright 2008 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
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
