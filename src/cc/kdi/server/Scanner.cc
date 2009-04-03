//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-02-26
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

#include <kdi/server/Scanner.h>
#include <kdi/server/Table.h>
#include <kdi/server/FragmentMerge.h>
#include <kdi/server/CellOutput.h>
#include <kdi/server/errors.h>
#include <kdi/rpc/PackedCellWriter.h>

using namespace kdi;
using namespace kdi::server;
using warp::Interval;
using std::string;
using std::vector;

namespace {
    
    void clipToFutureRows(ScanPredicate & pred, CellKey const & last)
    {
        pred = pred.clipRows(
            Interval<string>()
            .setLowerBound(last.getRow(), warp::BT_INCLUSIVE)
            .unsetUpperBound());
    }

    void clipToFutureRows(ScanPredicate & pred, Interval<string> const & last)
    {
        Interval<string> next;

        if(last.getUpperBound().isFinite())
            next.setLowerBound(last.getUpperBound().getAdjacentComplement())
                .unsetUpperBound();
        else
            next.setEmpty();

        pred = pred.clipRows(next);
    }
}

//----------------------------------------------------------------------------
// Scanner::PackedOutput
//----------------------------------------------------------------------------
class Scanner::PackedOutput
    : public CellOutput
{
public:
    void emitCell(strref_t row, strref_t column, int64_t timestamp,
                  strref_t value)
    {
        writer.append(row, column, timestamp, value);
    }

    void emitErasure(strref_t row, strref_t column, int64_t timestamp) {}
    size_t getCellCount() const { return writer.getCellCount(); }
    size_t getDataSize() const  { return writer.getDataSize(); }

    void reset() { writer.reset(); }
    void finish() { writer.finish(); }

    void getLastKey(CellKey & key) const
    {
        key.setRow(writer.getLastRow());
        key.setColumn(writer.getLastColumn());
        key.setTimestamp(writer.getLastTimestamp());
    }

    warp::StringRange getPacked() const { return writer.getPacked(); }

private:
    kdi::rpc::PackedCellWriter writer;
};


//----------------------------------------------------------------------------
// Scanner
//----------------------------------------------------------------------------
Scanner::Scanner(Table * table, BlockCache * cache,
                 ScanPredicate const & pred, ScanMode mode) :
    table(table),
    cache(cache),
    pred(pred),
    scanTxn(-1),
    output(new PackedOutput),
    haveLastKey(false),
    endOfScan(false)
{
    if(mode != SCAN_ANY_TXN)
        throw BadScanModeError();

    lock_t tableLock(table->tableMutex);
    table->addFragmentListener(this);
    table->addTabletListener(this);
}

Scanner::~Scanner()
{
    lock_t tableLock(table->tableMutex);
    table->removeTabletListener(this);
    table->removeFragmentListener(this);
}

void Scanner::scan_async(ScanCb * cb, size_t maxCells, size_t maxSize)
{
    try {
        // Clear the output buffer
        output->reset();

        // Clamp maxCells and maxSize to sane minimums
        if(!maxCells)
            maxCells = 1;
        if(maxSize <= output->getDataSize())
            maxSize = output->getDataSize() + 1;

        lock_t scannerLock(scannerMutex);

        if(merge || startMerge(scannerLock))
        {
            bool hasMore = merge->copyMerged(maxCells, maxSize, *output);
            if(!hasMore)
            {
                clipToFutureRows(pred, rows);
                merge.reset();
            }
        }

        endOfScan = pred.getRowPredicate()->isEmpty();

        // Finalize packed output and get last key, if any
        output->finish();
        if(output->getCellCount() > 0)
        {
            output->getLastKey(lastKey);
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

warp::StringRange Scanner::getPackedCells() const
{
    return output->getPacked();
}

bool Scanner::startMerge(lock_t & scannerLock)
{
    if(endOfScan)
        return false;

    // Clip the predicate so we ignore everything before lastKey, if
    // we have one
    if(haveLastKey)
        clipToFutureRows(pred, lastKey);

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
