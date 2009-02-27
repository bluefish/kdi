//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: kdi/server/Scanner.cc $
//
// Created 2009/02/26
//
// Copyright 2009 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#include <kdi/server/Scanner.h>

using namespace kdi;
using namespace kdi::server;

//----------------------------------------------------------------------------
// Scanner
//----------------------------------------------------------------------------
Scanner::Scanner(Table * table, ScanPredicate const & pred, ScanMode mode) :
    table(table),
    pred(pred),
    builder(),
    cells(&builder),
    scanTxn(-1),
    haveLastKey(false)
{
    if(mode != SCAN_ANY_TXN)
        throw BadScanModeError(mode);

    lock_t tableLock(table->tableMutex);
    table->addListener(this);
}

Scanner::~Scanner()
{
    lock_t tableLock(table->tableMutex);
    table->removeListener(this);
}

void Scanner::scan_async(ScanCb * cb, size_t maxCells, size_t maxSize)
{
    try {
        // Clear the output buffer
        cells.reset();
        packed.clear();

        // Clamp maxCells and maxSize to sane minimums
        if(!maxCells)
            maxCells = 1
        if(maxSize <= cells.getDataSize())
            maxSize = cells.getDataSize() + 1;

        lock_t scannerLock(scannerMutex);

        if(merge || startMerge(scannerLock))
        {
            bool hasMore = merge->copyMerged(maxCells, maxSize, cells);
            if(!hasMore)
            {
                clipToFutureTablets(pred, rows);
                merge.reset();
            }
        }

        endOfScan = pred.getRowPredicate()->isEmpty();

        // Build cells into packed
        builder.finalize();
        packed.resize(builder.getFinalSize());
        builder.exportTo(&packed[0]);

        // Get the last key
        CellBlock const * block =
            reinterpret_cast<CellBlock const *>(&packed[0]);
        if(!block->cells.empty())
        {
            CellData const * c = block->cells.end() - 1;
            lastKey.setRow(*c->key.row);
            lastKey.setColumn(*c->key.column);
            lastKey.setTimestamp(c->key.timestamp);
            haveLastKey = true;
        }

        // Return to callback
        cb->done();

    }
    catch(std::exception const & ex) {
        cb->error(ex);
    }
    catch(...) {
        cb->error(std::runtime_error("scan: unknown exception"));
    }
}

bool Scanner::startMerge(lock_t & scannerLock)
{
    if(endOfScan)
        return false;

    // Clip the predicate so we ignore everything before lastKey, if
    // we have one
    if(haveLastKey)
        clipToLastKey(pred, lastKey);

    // Need table lock -- unlock scanner lock first
    scannerLock.unlock();
    lock_t tableLock(table->tableMutex);
                
    // Get the last commit transaction for the table
    scanTxn = table->getLastCommitTxn();

    // Get the next fragment chain
    vector<Fragment const *> chain;
    table->getFirstFragmentChain(pred, chain, rows);

    // Done with table lock, need scanner lock again.
    scannerLock.lock();
    tableLock.unlock();

    // Open the merge on this chain.  Have it pick up after lastKey,
    // if we have one.
    merge.reset(new FragmentMerge(chain, cache, pred, getLastKey()));

    return true;
}

//----------------------------------------------------------------------------
// Scanner -- FragmentEventListener
//----------------------------------------------------------------------------
void Scanner::onNewFragment(
    Interval<string> const & r,
    Fragment const * f)
{
    // If we're using SCAN_LATEST_ROW_TXN, we should reset the
    // merge if the range overlaps.

    // For now, we don't care.
}

void Scanner::onReplaceFragments(
    Interval<string> const & r,
    vector<Fragment const *> const & f1,
    Fragment const * f2)
{
    lock_t scannerLock(scannerMutex);
        
    // If we're doing read isolation, we'll need some smartness
    // here so we don't wind up reading different transactions in
    // the same row.

    // Check to see if we care
    if(!merge || !r.overlaps(rows))
        return;

    // Clear the merge and reopen it later
    merge.reset();
}

//----------------------------------------------------------------------------
// Scanner -- TabletEventListener
//----------------------------------------------------------------------------
void Scanner::onTabletSplit(
    Interval<string> const & lo,
    Interval<string> const & hi)
{
    // We don't care if the tablet interval assignments change.
    // We only care if the range we're scanning gets dropped.
}

void Scanner::onTabletMerge(
    Interval<string> const & lo,
    Interval<string> const & hi)
{
    // We don't care if the tablet interval assignments change.
    // We only care if the range we're scanning gets dropped.
}

void Scanner::onTabletDrop(
    Interval<string> const & r)
{
    lock_t scannerLock(scannerMutex);
        
    // If the interval we're scanning gets dropped, we shouldn't
    // continue to supply results.  Reset and let the next call
    // for cells throw a TabletNotLoadedError.

    if(merge && r.overlaps(rows))
        merge.reset();
}
