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
#include <boost/bind.hpp>
#include <iostream>

#include <kdi/local/disk_table_writer.h>
using kdi::local::DiskTableWriter;

using namespace kdi;
using namespace kdi::tablet;
using namespace warp;
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
    //
    // For each active tablet in compaction set, remember:
    //   rangeMap:
    //      tablet range -> adjSeq (longest adjacent sequence
    //                              of fragments)
    //
    // For each fragment in compaction set, remember:
    //   invRangeMap:
    //      fragment -> adjRange (ranges referencing fragment in
    //                            an adjacent sequence)
    //
    // When merging from fragments, only scan adjRange
    //   for fragment,adjRange in invRangeMap:
    //      merge->pipeFrom(fragment.scan(adjRange))
    //
    // When replacing fragments for tablets, replace by ranges
    //   for range,adjSeq in rangeMap:
    //      if not range.overlaps(outRange):
    //         continue
    //      graph->replace(range, adjSeq, outFrag)
    //

    // graph::replace(range, adjSeq, outFrag):
    //    for tablet in (intersection of active tablets for each frag in adjSeq):
    //       -- guaranteed tablet contains adjSeq (1)
    //       if range contains tablet.rows:
    //          -- guaranteed we merged all relevant data (2)
    //          if outFrag contains data in tablet.rows:   -- optimization (3)
    //             tablet.replace(adjSeq, outFrag)   -- blow up if adjSeq not found
    //          else:
    //             tablet.remove(adjSeq)             -- blow up if adjSeq not found
    //
    // (1) Guarantee: All of the fragments in adjSeq are part of the
    //     current compaction set, so they are active and can't be
    //     replaced by something different with the same name.  If a
    //     tablet were to get dropped from this server, loaded by
    //     another server, compacted, dropped from that server, and
    //     reloaded by this server all while the compaction on this
    //     server was happening, then we have two cases: 1) at least
    //     one of fragments in the adjSeq was compacted and replaced
    //     for this tablet by another server, or 2) the other server
    //     didn't touch anything in adjSeq.  For 1), when the tablet
    //     gets reloaded it will not be in the active set for the
    //     replaced fragment.  The tablet will not be contained in the
    //     intersection of all active tablets for the fragment.  For
    //     2), we can go ahead with the replacement.
    //
    // (2) Guarantee: The input scan on each fragment in adjSeq
    //     includes all ranges for each tablet referencing the
    //     fragment (in that tablet's adjSeq).  There is no data in
    //     any of the fragments in adjSeq inside of the given range
    //     that is not included in outFrag.  This allows us to be
    //     confident about doing the right thing for recently split or
    //     joined tablets.
    //
    // (3) Optimization: it's possible outFrag doesn't contain
    //     anything in the tablet's range, and therefore doesn't need
    //     to be included in the tablet's fragment chain.  This seems
    //     most likely to happen when tablets split.

    // Get some info we need
    bool filterErasures;
    ConfigManagerPtr configMgr;
    FileTrackerPtr tracker;
    string table;
    size_t const outputSplitSize = size_t(1) << 30; // 1 GB
    {
        lock_t lock(dagMutex);
        filterErasures = fragDag.isRooted(fragments);
        Tablet * t = *fragDag.getActiveTablets(fragments).begin();
        configMgr = t->getConfigManager();
        table = t->getTableName();
        tracker = t->getFileTracker();
    }

    log("Compact thread: %srooted compaction on table %s",
        (filterErasures ? "" : "un"), table);

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

        log("Compact thread: merging from %s (%s)",
            (*i)->getFragmentUri(), pred);

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
            log("Compact thread: splitting output (sz=%s) at %s",
                sizeString(writer.size()), reprString(last.getRow()));

            // Reset readyToSplit to indicate we're no longer looking
            // to split the output
            readyToSplit = false;

            // Finalize output and reset to indicate we do not have an
            // active output
            writer.close();
            string uri = uriPushScheme(writerFn, "disk");
            outputOpen = false;

            // Close the output range with the last row added
            outputRange.setUpperBound(last.getRow().toString(), BT_INCLUSIVE);

            // Open new fragment
            FragmentPtr frag = configMgr->openFragment(uri);

            // Track the new file for automatic deletion
            FileTracker::AutoTracker autoTrack(*tracker, frag->getDiskUri());

            // Replace all compacted fragments in the output range
            // with a new fragment opened from the recent output.
            // Since split points are chosen to fall on tablet
            // boundaries, each tablet will map to no more than one
            // output.
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
        log("Compact thread: last output (sz=%s)", sizeString(writer.size()));

        // Finalize output and reset to indicate we do not have an
        // active output
        writer.close();
        string uri = uriPushScheme(writerFn, "disk");
        outputOpen = false;

        // Close the output range with the last row added
        outputRange.setUpperBound(last.getRow().toString(), BT_INCLUSIVE);

        // Open new fragment
        FragmentPtr frag = configMgr->openFragment(uri);

        // Track the new file for automatic deletion
        FileTracker::AutoTracker autoTrack(*tracker, frag->getDiskUri());

        // Replace all compacted fragments in the output range with a
        // new fragment opened from the recent output.  Since split
        // points are chosen to fall on tablet boundaries, each tablet
        // will map to no more than one output.
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
