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
#include <kdi/tablet/FileTracker.h>
#include <kdi/tablet/Fragment.h>
#include <kdi/cell_merge.h>
#include <kdi/scan_predicate.h>
#include <warp/functional.h>
#include <warp/uri.h>
#include <warp/log.h>
#include <warp/call_or_die.h>
#include <ex/exception.h>
#include <boost/bind.hpp>
#include <iostream>

#include <kdi/local/disk_table_writer.h>
using kdi::local::DiskTableWriter;

using namespace kdi;
using namespace kdi::tablet;
using namespace warp;
using namespace ex;
using namespace std;


//----------------------------------------------------------------------------
// SharedCompactor
//----------------------------------------------------------------------------
SharedCompactor::SharedCompactor() :
    cancel(false), enabled(true)
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

void SharedCompactor::disableCompactions()
{
    lock_t lock(mutex);
    log("Compact: disabling");
    enabled = false;
}

void SharedCompactor::enableCompactions()
{
    lock_t lock(mutex);
    log("Compact: enabling");
    enabled = true;
    wakeCond.notify_all();
}

void SharedCompactor::wakeup()
{
    lock_t lock(mutex);
    wakeCond.notify_all();
}

void SharedCompactor::shutdown()
{
    lock_t lock(mutex);
    if(!cancel)
    {
        log("SharedCompactor shutdown");
        cancel = true;
        wakeCond.notify_all();

        lock.unlock();
        thread->join();
    }
}

void SharedCompactor::compact(vector<FragmentPtr> const & fragments)
{
    log("Compact thread: compacting %d fragments", fragments.size());

    // External correctness depends on the following properties, but
    // this function doesn't actually care:
    //   - fragments is topologically ordered oldest to newest
    //   - fragments must be in same locality group (e.g. 'table')


    // XXX we have chosen a fragment set, but the active tablet set
    // can change on the fragments after we start compacting.  We'll
    // only see cells in the compaction from Tablets that are active
    // when we set up the merge.  If new tablets become active on the
    // fragments in different ranges, we don't want to cause them to
    // lose data by making them replace or remove.  Maybe need to
    // figure out fixed interval set at start and clip everything to
    // that (remove included).  currently seeing a problem where a
    // tablet can't find its replacement sequence.  this is probably
    // because it came in later and the filtered fragment sequence is
    // a subsequence but not a substring of its tablet list.

    // Choosing a fully adjacent set is hard




    // Split outputs if they get bigger than this:
    size_t const outputSplitSize = size_t(1) << 30; // 1 GB

    // Random stuff we need to operate.  It probably should be
    // abstracted away or passed in, but since we're in hack mode,
    // we'll just pull it from the first Tablet we see.
    ConfigManagerPtr configMgr;
    FileTrackerPtr tracker;
    string table;


    // For each active tablet in compaction set, remember:
    //   rangeMap:  (not an actual std::map)
    //      tablet range -> adjSeq (longest adjacent sequence
    //                              of fragments)

    typedef std::vector<FragmentPtr> fragment_vec;
    typedef std::pair<Interval<string>, fragment_vec> adj_pair;
    typedef std::vector<adj_pair> adj_vec;

    bool allRooted = true;
    adj_vec rangeMap;
    {
        lock_t dagLock(dagMutex);

        // Get all active tablets for fragment set
        FragDag::tablet_set active = fragDag.getActiveTablets(fragments);

        // For each tablet, figure out longest adjacent fragment
        // sequence and remember that for the tablet's range
        for(FragDag::tablet_set::const_iterator t = active.begin();
            t != active.end(); ++t)
        {
            // Find max-length adjacent sequence
            fragment_vec adjSeq =
                fragDag.maxAdjacentTabletFragments(fragments, *t);

            // Unless the sequence includes at least two fragments,
            // there's no point in compacting it
            if(adjSeq.size() < 2)
                continue;

            // Remember the sequence for the range
            rangeMap.push_back(
                adj_pair(
                    (*t)->getRows(),
                    adjSeq));

            // If the first element in the sequence has a parent, then
            // the sequence is not rooted
            if(fragDag.getParent(adjSeq.front(), *t))
                allRooted = false;
        }

        // Since we're in the neighborhood, pull our random parameters
        // from one of the tablets.  It doesn't matter which as all of
        // the tablets should have the same information.
        if(!active.empty())
        {
            Tablet * t = *active.begin();
            configMgr = t->getConfigManager();
            tracker = t->getFileTracker();
            table = t->getTableName();
        }
    }

    // If we didn't find anything to compact, we're kind of hosed.
    if(rangeMap.empty())
        raise<ValueError>("fragment set contained no compactible sequences");


    // For each fragment in compaction set, remember:
    //   invRangeMap:
    //      fragment -> adjRange (ranges referencing fragment in
    //                            an adjacent sequence)

    typedef std::map<FragmentPtr, IntervalSet<string> > inv_range_map;

    // Invert rangeMap to build invRangeMap
    inv_range_map invRangeMap;
    for(adj_vec::const_iterator i = rangeMap.begin();
        i != rangeMap.end(); ++i)
    {
        Interval<string> const & range = i->first;
        fragment_vec const & adjSeq = i->second;

        for(fragment_vec::const_iterator f = adjSeq.begin();
            f != adjSeq.end(); ++f)
        {
            invRangeMap[*f].add(range);
        }
    }


    log("Compact thread: %srooted compaction on table %s",
        (allRooted ? "" : "un"), table);

    // no writer, loader.  use configMgr + DiskTableWriter instead
    DiskTableWriter writer(64<<10);
    string writerFn;


    // When merging from fragments, only scan adjRange
    //   for fragment,adjRange in invRangeMap:
    //      merge->pipeFrom(fragment.scan(adjRange))

    /// Build a merge over the compaction fragments.  Filter erasures
    /// if the compaction set is rooted.
    CellStreamPtr merge = CellMerge::make(allRooted);
    for(vector<FragmentPtr>::const_reverse_iterator i = fragments.rbegin();
        i != fragments.rend(); ++i)
    {
        inv_range_map::const_iterator mi = invRangeMap.find(*i);
        if(mi == invRangeMap.end())
        {
            // This fragment isn't included in any adjacent sequence,
            // so don't bother.
            continue;
        }

        // Only scan the parts of the fragment active in the current
        // compaction
        ScanPredicate pred;
        pred.setRowPredicate(mi->second);

        log("Compact thread: merging from %s (%s)",
            (*i)->getFragmentUri(), pred);

        merge->pipeFrom((*i)->scan(pred));
    }

    // Bookkeeping: output
    Interval<string> outputRange;
    outputRange.setInfinite();
    bool outputOpen = false;
    bool readyToSplit = false;

    // Read cells in merged order
    IntervalPointOrder<warp::less> lt;
    Cell x;
    while(merge->get(x))
    {
        // If we're looking for a place to split the output and the
        // current cell is outside our outputRange, perform the split.
        if(readyToSplit && lt(outputRange.getUpperBound(), x.getRow()))
        {
            log("Compact thread: splitting output (sz=%s) at %s",
                sizeString(writer.size()),
                reprString(outputRange.getUpperBound().getValue()));

            // Finalize output and reset to indicate we do not have an
            // active output
            writer.close();
            string uri = uriPushScheme(writerFn, "disk");
            outputOpen = false;

            // Open the newly written fragment
            FragmentPtr frag = configMgr->openFragment(uri);

            // Track the new file for automatic deletion
            FileTracker::AutoTracker autoTrack(*tracker, frag->getDiskUri());

            // Replace the adjacent fragment sequence for all mapped
            // ranges in the output with the new fragment.
            {
                lock_t dagLock(dagMutex);

                for(adj_vec::const_iterator i = rangeMap.begin();
                    i != rangeMap.end(); ++i)
                {
                    Interval<string> const & range = i->first;
                    fragment_vec const & adjSeq = i->second;

                    // If mapped range and output range don't overlap,
                    // wait for a different output -- all ranges will
                    // be covered eventually
                    if(!range.overlaps(outputRange))
                        continue;

                    // It's possible the mapped range isn't entirely
                    // contained in outputRange if a tablet split
                    // happened after building the rangeMap.  Clip the
                    // range to outputRange and update the clipped
                    // range.  The other part of the range will be (or
                    // was already) updated in a different output.
                    Interval<string> clippedRange(range);
                    clippedRange.clip(outputRange);
                    fragDag.replaceFragments(clippedRange, adjSeq, frag);
                }
            }

            // Reset readyToSplit to indicate we're no longer looking
            // to split the output
            readyToSplit = false;

            // The upper bound will be finite or it wouldn't have been
            // less than the current row.
            assert(outputRange.getUpperBound().isFinite());

            // Reset outputRange to the half interval immediately
            // above the current output range
            outputRange.setLowerBound(
                outputRange.getUpperBound().getAdjacentComplement());
            outputRange.setUpperBound(string(), BT_INFINITE);
        }

        // If we don't have an active output, open a new one
        if(!outputOpen)
        {
            // Start an output file for the compacting table
            writerFn = configMgr->getDataFile(table);
            writer.open(writerFn);
            outputOpen = true;
        }

        // Add another cell to the output
        writer.put(x);

        // Check to see if the output is large enough to split if
        // haven't already chosen one.
        if(!readyToSplit && writer.size() > outputSplitSize)
        {
            // Choose a split point on a tablet boundary and restrict
            // outputRange.  The last row we added to the output forms
            // an inclusive lower bound for the split point.  We'll
            // split the first time we see a cell outside outputRange.
            {
                lock_t lock(dagMutex);
                outputRange.setUpperBound(
                    fragDag.findNextSplit(fragments, x.getRow()));
            }
            readyToSplit = true;
        }
    }

    // We're done merging.  If we still have an output open, finish
    // it.
    if(outputOpen)
    {
        log("Compact thread: last output (sz=%s)", sizeString(writer.size()));

        // Finalize output and reset to indicate we do not have an
        // active output
        writer.close();
        string uri = uriPushScheme(writerFn, "disk");
        outputOpen = false;

        // Open the newly written fragment
        FragmentPtr frag = configMgr->openFragment(uri);

        // Track the new file for automatic deletion
        FileTracker::AutoTracker autoTrack(*tracker, frag->getDiskUri());

        // Replace the adjacent fragment sequence for all mapped
        // ranges in the output with the new fragment.
        {
            lock_t dagLock(dagMutex);

            for(adj_vec::const_iterator i = rangeMap.begin();
                i != rangeMap.end(); ++i)
            {
                Interval<string> const & range = i->first;
                fragment_vec const & adjSeq = i->second;

                // If mapped range and output range don't overlap,
                // wait for a different output -- all ranges will be
                // covered eventually
                if(!range.overlaps(outputRange))
                    continue;

                // It's possible the mapped range isn't entirely
                // contained in outputRange if a tablet split happened
                // after building the rangeMap.  Clip the range to
                // outputRange and update the clipped range.  The
                // other part of the range will be (or was already)
                // updated in a different output.
                Interval<string> clippedRange(range);
                clippedRange.clip(outputRange);
                fragDag.replaceFragments(clippedRange, adjSeq, frag);
            }
        }
    }

    // Now that we're done, sweep the graph to get rid of old
    // fragments
    {
        lock_t dagLock(dagMutex);
        fragDag.sweepGraph();
    }
}

void SharedCompactor::compactLoop()
{
    lock_t lock(mutex);
    while(!cancel)
    {
        // Make sure we're enabled
        if(!enabled)
        {
            log("Compact thread: disabled, waiting");
            wakeCond.wait(lock);
            continue;
        }

        // Get a compaction set
        log("Compact thread: choosing compaction set");
        lock_t dagLock(dagMutex);
        vector<FragmentPtr> frags = fragDag.chooseCompactionList();
        dagLock.unlock();

        if(frags.empty())
        {
            // If empty, wait for a compaction request
            log("Compact thread: nothing to compact, waiting");
            wakeCond.wait(lock);
            continue;
        }
        else
        {
            // Else, compact it
            lock.unlock();
            compact(frags);
            lock.lock();
        }
    }
}
