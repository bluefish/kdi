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
#include <warp/timer.h>
#include <ex/exception.h>
#include <boost/bind.hpp>
#include <iostream>
#include <vector>

#include <kdi/local/disk_table_writer.h>
using kdi::local::DiskTableWriter;

#define COMPACTOR_DEBUG
#ifdef COMPACTOR_DEBUG
#include <fstream>
#include <sstream>
#include <warp/strutil.h>
namespace
{
    size_t compactionId = 0;
}
#endif

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

void SharedCompactor::compact(set<FragmentPtr> const & fragments)
{
#ifdef COMPACTOR_DEBUG
    FragDag::fragment_set debugFragments(fragments.begin(), fragments.end());
#endif

    log("Compact thread: compacting %d fragments", fragments.size());
    WallTimer timer;
    size_t outputCells = 0;
    size_t outputFragments = 0;
    size_t outputSize = 0;
    size_t inputFragments = fragments.size();
    size_t inputSize = 0;            // active parts
    size_t restrictedFragments = 0;
    size_t restrictedSize = 0;

    // External correctness depends on the following properties, but
    // this function doesn't actually care:
    //   - fragments is topologically ordered oldest to newest
    //   - fragments must be in same locality group (e.g. 'table')

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

    typedef std::set<FragmentPtr> fragment_set;
    typedef std::vector<FragmentPtr> fragment_vec;
    typedef std::pair<fragment_vec, bool> adj_list;  // (frag-list, isRooted)
    typedef std::pair<Interval<string>, adj_list> adj_pair;
    typedef std::vector<adj_pair> adj_vec;

    adj_vec rangeMap;
    {
        lock_t dagLock(dagMutex);

        for(fragment_set::const_iterator f = fragments.begin();
            f != fragments.end(); ++f)
        {
            inputSize += fragDag.getActiveSize(*f);
        }

        // Get all active tablets for fragment set
        FragDag::tablet_set active = fragDag.getActiveTablets(fragments);

        // For each tablet, figure out longest adjacent fragment
        // sequence and remember that for the tablet's range
        for(FragDag::tablet_set::const_iterator t = active.begin();
            t != active.end(); ++t)
        {
            // Find max-length adjacent sequence
            fragment_vec adjSeq = (*t)->getMaxAdjacentChain(fragments);

            // Unless the sequence includes at least two fragments,
            // there's no point in compacting it
            if(adjSeq.size() < 2)
                continue;

            // See if the sequence is rooted (doesn't have a parent)
            bool isRooted = !(fragDag.getParent(adjSeq.front(), *t));

            // Remember the sequence for the range
            rangeMap.push_back(
                adj_pair(
                    (*t)->getRows(),
                    adj_list(
                        adjSeq,
                        isRooted)));
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

#ifdef COMPACTOR_DEBUG
    {
        ostringstream oss;
        oss << "compact." << compactionId << "." << table << ".A.txt";
        string fn = replace(oss.str(), "/", "_");
        ofstream out(fn.c_str());

        lock_t dagLock(dagMutex);
        fragDag.dumpDotGraph(out, debugFragments, false);
        dagLock.unlock();

        out.close();
    }
#endif

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
        fragment_vec const & adjSeq = i->second.first;

        for(fragment_vec::const_iterator f = adjSeq.begin();
            f != adjSeq.end(); ++f)
        {
            invRangeMap[*f].add(range);
        }
    }

    log("Compact thread: starting compaction on table %s", table);

    // no writer, loader.  use configMgr + DiskTableWriter instead
    DiskTableWriter writer(64<<10);
    string writerFn;


    // New plan:
    //
    // When merging, allow order (and membership) of merge inputs to
    // change at every range boundary.  To do this we need to make
    // sure that each input to the merge stops returning cells when
    // range boundaries are reached.  When we hit the end of stream on
    // the merge, that means we're at the end of the range.  We take
    // all the inputs necessary for the next range, notify them they
    // should now return cells in the new range, and build a new merge
    // in the order necessary for the new range.


    // When reading from fragments, only scan adjRange
    //   for fragment,adjRange in invRangeMap:
    //      inputMap[fragment] = fragment.scan(adjRange)

    typedef std::map<FragmentPtr, CellStreamPtr> input_map;
    input_map inputMap;

    /// Open all active inputs on the intervals involved in compactions.
    for(inv_range_map::const_iterator i = invRangeMap.begin();
        i != invRangeMap.end(); ++i)
    {
        FragmentPtr const & fragment = i->first;
        IntervalSet<string> const & activeRows = i->second;

        ++restrictedFragments;
        restrictedSize += fragment->getDiskSize(activeRows);

        // Only scan the parts of the fragment active in the current
        // compaction
        ScanPredicate pred;
        pred.setRowPredicate(activeRows);

        log("Compact thread: merging from %s (%s)",
            fragment->getFragmentUri(), pred);

        inputMap[fragment] = fragment->scan(pred);
    }

    // Bookkeeping: output
    Interval<string> outputRange;
    outputRange.setInfinite();
    bool outputOpen = false;
    bool readyToSplit = false;

    // Read cells in merged order
    IntervalPointOrder<warp::less> lt;

    // Merge one range at a time
    for(adj_vec::const_iterator i = rangeMap.begin();
        i != rangeMap.end(); ++i)
    {
        Interval<string> const & range = i->first;
        fragment_vec const & adjSeq = i->second.first;
        bool isRooted = i->second.second;

        log("Compact thread: compacting table %s, range: %s",
            table, ScanPredicate().setRowPredicate(IntervalSet<string>().add(range)));

        // Build a merge from all inputs involved
        CellStreamPtr merge = CellMerge::make(isRooted);
        for(fragment_vec::const_reverse_iterator f = adjSeq.rbegin();
            f != adjSeq.rend(); ++f)
        {
            // Get the input stream for the fragment
            CellStreamPtr & inputStream = inputMap[*f];

            // Advance limits on each active input
            if(range.getUpperBound().isFinite())
            {
                //inputStream->setLimit(range.getUpperBound());
            }
            else
            {
                //inputStream->clearLimit();
            }

            // Add the input to the merge
            merge->pipeFrom(inputStream);
        }

        // Merge until we're out of input (i.e. done with the range)
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
                ++outputFragments;
                string uri = uriPushScheme(writerFn, "disk");
                outputOpen = false;

                // Open the newly written fragment
                FragmentPtr frag = configMgr->openFragment(uri);
                outputSize += frag->getDiskSize(Interval<string>().setInfinite());

#ifdef COMPACTOR_DEBUG
                debugFragments.insert(frag);
#endif

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
                        fragment_vec const & adjSeq = i->second.first;

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
        ++outputCells;

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
        ++outputFragments;
        string uri = uriPushScheme(writerFn, "disk");
        outputOpen = false;

        // Open the newly written fragment
        FragmentPtr frag = configMgr->openFragment(uri);
        outputSize += frag->getDiskSize(Interval<string>().setInfinite());

#ifdef COMPACTOR_DEBUG
        debugFragments.insert(frag);
#endif

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
                fragment_vec const & adjSeq = i->second.first;

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

    int64_t ms = timer.getElapsedNs() / 1000000;
    log("Compact stats: in %d %d rin %d %d out %d %d cells %d ms %d",
        inputFragments, inputSize,
        restrictedFragments, restrictedSize,
        outputFragments, outputSize,
        outputCells, ms);

#ifdef COMPACTOR_DEBUG
    {
        ostringstream oss;
        oss << "compact." << compactionId << "." << table << ".B.txt";
        string fn = replace(oss.str(), "/", "_");
        ofstream out(fn.c_str());

        lock_t dagLock(dagMutex);
        fragDag.dumpDotGraph(out, debugFragments, false);
        dagLock.unlock();

        out.close();

        ++compactionId;
    }
#endif
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
        set<FragmentPtr> frags = fragDag.chooseCompactionSet();
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
