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

#include "Scanner.h"

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
    scanTxn(-1)
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

        bool firstRange = !merge;
        for(;;)
        {
            if(!merge)
            {
                if(!startMerge(firstRange, scannerLock))
                    break;
                firstRange = false;
            }

            bool eos = merge->more(maxCells - cells.getCellCount(),
                                   maxSize - cells.getDataSize(),
                                   cells, lastKey);
            
            if(eos)
            {
                clipToRemainingRows();
                merge.reset();
            }

            if(cells.getCellCount() >= maxCells ||
               cells.getDataSize() >= maxSize)
            {
                break;
            }
        }

        endOfScan = pred.getRowPredicate()->isEmpty();

        // Build cells into packed
        builder.finalize();
        packed.clear();
        packed.resize(builder.getFinalSize());
        builder.exportTo(&packed[0]);
    }
    catch(std::exception const & ex) {
        cb->error(ex);
    }
    catch(...) {
        cb->error(std::runtime_error("scan: unknown exception"));
    }
}

bool Scanner::startMerge(bool firstMerge, lock_t & scannerLock)
{
    if(endOfScan)
        return false;

    // Need table lock -- unlock scanner lock first
    scannerLock.unlock();
    lock_t tableLock(table->tableMutex);
                
    // Get the last commit transaction for the table
    int64_t prevTxn = scanTxn;
    scanTxn = table->getLastCommitTxn();

    // Get the next fragment chain
    vector<Fragment const *> chain;
    table->getFirstFragmentChain(pred, chain, rows);

    // Done with table lock, need scanner lock again.
    scannerLock.lock();
    tableLock.unlock();

    // Open the merge after lastKey
    merge.reset(new FragmentMerge(chain, lastKey));

    // If we've already returned some data, we want to make sure
    // future data is from the same transaction.
    if(!firstRange && scanTxn > prevTxn)
    {
        // The table has moved on since our first scan -- has this
        // fragment chain?
        if(getMaxTxn(chain) > prevTxn)
        {
            // This chain has newer data.  Return what we have and
            // wait for the next call before serving this chain.
            return false;
        }
        else
        {
            // This chain hasn't had any updates more recent than our
            // previous scan transaction.  Clamp the transaction
            // number and continue.
            scanTxn = prevTxn;
        }
    }

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
