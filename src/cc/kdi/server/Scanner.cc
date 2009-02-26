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
Scanner::Scanner(Table * table, ScanPredicate const & pred, ScanMode mode)
{
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

        // Clamp maxCells and maxSize to sane minimums
        if(!maxCells)
            maxCells = 1
        if(maxSize <= cells.getDataSize())
            maxSize = cells.getDataSize() + 1;


        lock_t scannerLock(scannerMutex);

        // Open first range:
        //   maxTxn = getLastCommitTxn()  (may be >> than getMaxTxn(frags))
        
        // Open subsequent range:
        //   lastTxn = getLastCommitTxn()
        //   if lastTxn > maxTxn and getMaxTxn(frags) > maxTxn:
        //      break  (don't want to mix newer results with old)

        // Return maxTxn
        
        bool firstRange = !merge;
        int64_t maxTxn = scanTxn;

        bool scanComplete = false;
        bool noTablet = false;
        for(;;)
        {
            // If merge is not active, set it up
            if(!merge)
            {
                pred = pred.clipRows(lastRow);
                if(pred.getRowPredicate()->isEmpty())
                {
                    scanComplete = true;
                    break;
                }

                // Need table lock -- unlock scanner lock first
                scannerLock.unlock();
                lock_t tableLock(table->tableMutex);
                
                // Get the last commit transaction for the table
                scanTxn = table->getLastCommitTxn();

                // Get the next fragment chain
                vector<Fragment const *> chain;
                if(!table->getFirstFragmentChain(pred, chain, rows))
                {
                    // The next range isn't on this server
                    noTablet = true;
                    break;
                }

                // Done with table lock, need scanner lock again.
                scannerLock.lock();
                tableLock.unlock();

                // Open the merge
                merge.reset(new FragmentMerge(chain));

                // If this is the first range we've scanned, set the
                // max txn for the batch
                if(firstRange)
                {
                    maxTxn = scanTxn;
                    firstRange = false;
                }
                // Else if the max transaction for the new chain is
                // greater than the transaction on the batch so far,
                // end the batch.  We don't want to mix old data with
                // new.
                else if(scanTxn > maxTxn && getMaxTxn(chain) > maxTxn)
                {
                    break;
                }
            }

            assert(merge);

            // We have an active merge, get some (more) cells
            bool eos = merge->next(maxCells - cells.getCellCount(),
                                   maxSize - cells.getDataSize(),
                                   cells, lastKey);

            // Did we get to the end of this range?
            if(eos)
            {
                merge.reset();
                
                

                if(rows.getUpperBound().isFinite())
                {
                    pred = pred.clip(
                        Interval<string>()
                        .setLowerBound(
                            rows.getUpperBound().getAdjacentComplement()
                            )
                        .unsetUpperBound());
                }
                
                // Unregister 
            }

            if(cells.getCellCount() >= maxCells ||
               cells.getDataSize() >= maxSize)
            {
                break;
            }
        }

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
