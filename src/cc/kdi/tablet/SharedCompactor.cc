//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
// Created 2008-05-21
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

#include <kdi/tablet/SharedCompactor.h>
#include <kdi/tablet/Tablet.h>
#include <kdi/tablet/ConfigManager.h>
#include <warp/log.h>
#include <warp/call_or_die.h>
#include <boost/bind.hpp>
#include <iostream>

using namespace kdi;
using namespace kdi::tablet;
using namespace warp;
using namespace std;

//----------------------------------------------------------------------------
#include <kdi/tablet/Fragment.h>
#include <kdi/cell_merge.h>
#include <kdi/scan_predicate.h>
#include <warp/functional.h>
#include <boost/scoped_ptr.hpp>

using namespace kdi;
using namespace kdi::tablet;
using namespace warp;
using namespace std;
//----------------------------------------------------------------------------

#include <kdi/local/disk_table_writer.h>
using kdi::local::DiskTableWriter;



//----------------------------------------------------------------------------
// SharedCompactor
//----------------------------------------------------------------------------
SharedCompactor::SharedCompactor() :
    cancel(false)
{
    thread.reset(
        new boost::thread(
            callOrDie(
                boost::bind(
                    &SharedCompactor::compactLoop,
                    this
                    ),
                "Compact thread", true
                )
            )
        );

    log("SharedCompactor %p: created", this);
}
    
SharedCompactor::~SharedCompactor()
{
    shutdown();

    log("SharedCompactor %p: destroyed", this);
}

void SharedCompactor::requestCompaction(TabletPtr const & tablet)
{
    lock_t lock(mutex);
    requestAdded.notify_one();
}

void SharedCompactor::shutdown()
{
    lock_t lock(mutex);
    if(!cancel)
    {
        log("SharedCompactor shutdown");
        cancel = true;
        requestAdded.notify_all();

        lock.unlock();
        thread->join();
    }
}

void SharedCompactor::compact(vector<FragmentPtr> const & fragments)
{
    // Get some info we need
    bool filterErasures;
    ConfigManagerPtr configMgr;
    string table;
    size_t const outputSplitSize = size_t(1) << 30; // 1 GB
    {
        lock_t lock(dagMutex);
        filterErasures = fragDag.isRooted(fragments);
        Tablet * t = *fragDag.getActiveTablets(fragments).begin();
        configMgr = t->getConfigManager();
        table = t->getTableName();
    }

    // no writer, loader.  use configMgr + DiskTableWriter instead
    DiskTableWriter writer(64<<10);
    string writerFn;


    // External correctness depends on the following properties, but
    // this function doesn't actually care:
    //   - fragments must be adjacent (see chooseCompactionFragments())
    //   - fragments is topologically ordered oldest to newest
    //   - fragments must be in same locality group (e.g. 'table')
    //   - filterErasures <==> isRooted(fragments)

    /// Build a merge over the compaction fragments.  Filter erasures
    /// if the compaction set is rooted.
    CellStreamPtr merge = CellMerge::make(filterErasures);
    for(vector<FragmentPtr>::const_reverse_iterator i = fragments.rbegin();
        i != fragments.rend(); ++i)
    {
        // Only scan the parts of the fragment covered by active
        // tablets.  If the file is shared with external tablets, it's
        // possible there's more to the fragment that we can ignore.
        // Also, if the fragment contains stale/unactive cells in one
        // of our other tablet ranges (maybe it used to be active and
        // was compacted earlier), we don't want to get confused by
        // pulling that data back in.
        ScanPredicate pred;

        {
            lock_t lock(dagMutex);
            pred.setRowPredicate(
                fragDag.getActiveRanges(*i)
                );
        }
        merge->pipeFrom((*i)->scan(pred));
    }

    // Bookkeeping: output
    Interval<string> outputRange;
    bool outputOpen = false;

    // Bookkeeping: split
    IntervalPoint<string> nextSplit;
    bool readyToSplit = false;

    // Read cells in merged order
    IntervalPointOrder<warp::less> lt;
    Cell last;
    Cell x;
    while(merge->get(x))
    {
        // If we're looking for a place to split the output and the
        // current cell crosses the boundary, perform the output
        // split.
        if(readyToSplit && lt(nextSplit, x.getRow()))
        {
            // Reset readyToSplit to indicate we're no longer looking
            // to split the output
            readyToSplit = false;

            // Finalize output and reset to indicate we do not have an
            // active output
            writer.close();
            string uri = writerFn;
            outputOpen = false;

            // Close the output range with the last row added
            outputRange.setUpperBound(last.getRow().toString(), BT_INCLUSIVE);

            // Replace all compacted fragments in the output range
            // with a new fragment opened from the recent output.
            // Since split points are chosen to fall on tablet
            // boundaries, each tablet will map to no more than one
            // output.
            FragmentPtr frag = configMgr->openFragment(uri);
            {
                lock_t lock(dagMutex);
                fragDag.replaceFragments(fragments, frag, outputRange);
            }
        }

        // If we don't have an active output, open a new one
        if(!outputOpen)
        {
            // Start an output file for the compacting table
            writerFn = configMgr->getDataFile(table);
            writer.open(writerFn);
            outputOpen = true;

            // Open the output range with the first row we're about to
            // put into the output
            outputRange.setLowerBound(x.getRow().toString(), BT_INCLUSIVE);
        }

        // Add another cell to the output
        writer.put(x);

        // Remember the last cell we added
        last = x;

        // Check to see if the output is large enough to split if
        // haven't already chosen one.
        if(!readyToSplit && writer.size() > outputSplitSize)
        {
            // Choose a split point on a tablet boundary.  The last
            // row we added to the output forms an inclusive lower
            // bound for the split point.  We'll split the first time
            // we see a cell with a row greater than nextSplit.
            {
                lock_t lock(dagMutex);
                nextSplit = fragDag.findNextSplit(fragments, last.getRow());
            }
            readyToSplit = true;
        }
    }

    // We're done merging.  If we still have an output open, finish
    // it.
    if(outputOpen)
    {
        // Finalize output and reset to indicate we do not have an
        // active output
        writer.close();
        string uri = writerFn;
        outputOpen = false;

        // Close the output range with the last row added
        outputRange.setUpperBound(last.getRow().toString(), BT_INCLUSIVE);

        // Replace all compacted fragments in the output range with a
        // new fragment opened from the recent output.  Since split
        // points are chosen to fall on tablet boundaries, each tablet
        // will map to no more than one output.
        FragmentPtr frag = configMgr->openFragment(uri);
        {
            lock_t lock(dagMutex);
            fragDag.replaceFragments(fragments, frag, outputRange);
        }
    }

    // Remove the fragments from the graph.  Any tablets referencing
    // these fragments that didn't get one of the previous calls to
    // replaceFragments() didn't overlap with any of the output files.
    // Therefore the compaction set didn't contain anything belonging
    // to these tablets and they should drop their references.
    {
        lock_t lock(dagMutex);
        fragDag.removeFragments(fragments);
    }
}

void SharedCompactor::compactLoop()
{
    for(;;)
    {
        // Check for cancelation
        {
            lock_t lock(mutex);
            if(cancel)
                break;
        }

        // Get a compaction set
        lock_t dagLock(dagMutex);
        vector<FragmentPtr> frags = fragDag.chooseCompactionList();
        dagLock.unlock();

        // If empty, wait for a compaction request
        if(frags.empty())
        {
            lock_t lock(mutex);
            if(cancel)
                break;
            requestAdded.wait(lock);
            continue;
        }

        // Else, compact it
        compact(frags);
    }
}
