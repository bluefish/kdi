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
#include <flux/cutoff.h>
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

//#define COMPACTOR_DEBUG
#ifdef COMPACTOR_DEBUG
#include <fstream>
#include <sstream>
#include <warp/strutil.h>

namespace {
    size_t compactionId = 0;
}
#endif


using namespace kdi;
using namespace kdi::tablet;
using namespace warp;
using namespace ex;
using namespace std;

namespace {

    // Split outputs if they get bigger than this:
    size_t const OUTPUT_SPLIT_SIZE = size_t(1) << 30;  // 1 GB

    typedef std::set<FragmentPtr> fragment_set;
    typedef std::vector<FragmentPtr> fragment_vec;

    struct CellRowCutoff
    {
        IntervalPointOrder<warp::less> lt;

        bool operator()(Cell const & a, IntervalPoint<string> const & b) const
        {
            return lt(a.getRow(), b);
        }
    };

    struct CompactRange
    {
        Interval<string> const range;
        fragment_vec fragments;
        bool isRooted;

        CompactRange(Interval<string> const & range) :
            range(range), isRooted(false)
        {
        }
    };

    typedef boost::shared_ptr<CompactRange> CompactRangePtr;

    struct CompactRangePtrLt
    {
        bool operator()(CompactRangePtr const & a, CompactRangePtr const & b) const
        {
            return a->range.getUpperBound() < b->range.getUpperBound();
        }
    };

    typedef std::vector<CompactRangePtr> range_vec;

}


//----------------------------------------------------------------------------
// CompactionOutput
//----------------------------------------------------------------------------
namespace {

    class CompactionOutput
    {
        typedef boost::mutex::scoped_lock lock_t;

        ConfigManagerPtr configMgr;
        FileTrackerPtr tracker;
        boost::mutex & dagMutex;
        FragDag & fragDag;
        string table;
        fragment_set const & fragments;
        range_vec const & rangeMap;

        DiskTableWriter writer;
        string writerFn;

        Interval<string> outputRange;
        bool outputOpen;
        bool readyToSplit;

        size_t nFragments;
        size_t outputSize;

        IntervalPointOrder<warp::less> lt;

#ifdef COMPACTOR_DEBUG
    public:
        fragment_set newFragments;
#endif

    public:
        // Future refactor notes:
        //
        //   ConfigManager and table name could be rolled into
        //   FragmentWriter interface.  That would take away our
        //   internal disk writer, writerFn, etc.
        //
        //   Instead of having the fragment set, we might want some
        //   kind of SplitFinder interface encapsulating the mutex,
        //   fragDag, and fragment set.
        //
        //   Could have a replacement callback when compaction is
        //   done, which would encapsulate the tracker, dagMutex, and
        //   fragDag.

        CompactionOutput(ConfigManagerPtr const & configMgr,
                         FileTrackerPtr const & tracker,
                         boost::mutex & dagMutex,
                         FragDag & fragDag,
                         string const & table,
                         fragment_set const & fragments,
                         range_vec const & rangeMap
            ) :
            configMgr(configMgr),
            tracker(tracker),
            dagMutex(dagMutex),
            fragDag(fragDag),
            table(table),
            fragments(fragments),
            rangeMap(rangeMap),
            writer(64<<10),
            outputOpen(false),
            readyToSplit(false),
            nFragments(0),
            outputSize(0)
        {
            outputRange.setInfinite();
        }

        void put(Cell const & x)
        {
            // If we're looking for a place to split the output and the
            // current cell is outside our outputRange, perform the split.
            if(readyToSplit && lt(outputRange.getUpperBound(), x.getRow()))
            {
                log("Compact thread: splitting output (sz=%s) at %s",
                    sizeString(writer.size()),
                    reprString(outputRange.getUpperBound().getValue()));

                finalizeOutput();
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
            if(!readyToSplit && writer.size() > OUTPUT_SPLIT_SIZE)
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

        void flush()
        {
            if(outputOpen)
            {
                log("Compact thread: last output (sz=%s)", sizeString(writer.size()));
                finalizeOutput();
            }
        }

        size_t getOutputFragmentCount() const { return nFragments; }
        size_t getOutputSize() const { return outputSize; }

    private:
        void finalizeOutput()
        {
            // Finalize output and reset to indicate we do not have an
            // active output
            writer.close();
            ++nFragments;
            string uri = uriPushScheme(writerFn, "disk");
            outputOpen = false;

            // Open the newly written fragment
            FragmentPtr frag = configMgr->openFragment(uri);
            outputSize += frag->getDiskSize(Interval<string>().setInfinite());

#ifdef COMPACTOR_DEBUG
            newFragments.insert(frag);
#endif

            // Track the new file for automatic deletion
            FileTracker::AutoTracker autoTrack(*tracker, frag->getDiskUri());

            // Replace the adjacent fragment sequence for all mapped
            // ranges in the output with the new fragment.
            {
                lock_t dagLock(dagMutex);

                for(range_vec::const_iterator i = rangeMap.begin();
                    i != rangeMap.end(); ++i)
                {
                    Interval<string> const & range = (*i)->range;
                    fragment_vec const & adjSeq = (*i)->fragments;

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

            if(outputRange.getUpperBound().isFinite())
            {
                // Reset outputRange to the half interval immediately
                // above the current output range
                outputRange.setLowerBound(
                    outputRange.getUpperBound().getAdjacentComplement());
                outputRange.setUpperBound(string(), BT_INFINITE);
            }
        }
    };
}


//----------------------------------------------------------------------------
// SharedCompactor
//----------------------------------------------------------------------------
SharedCompactor::SharedCompactor() :
    disabled(0),
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

void SharedCompactor::disableCompactions()
{
    lock_t lock(mutex);
    ++disabled;
    log("Compact: disabling (count=%d)", disabled);
}

void SharedCompactor::enableCompactions()
{
    lock_t lock(mutex);
    if(!disabled)
        raise<RuntimeError>("compactor is not disabled");

    if(!--disabled)
    {
        log("Compact: enabling");
        wakeCond.notify_all();
    }
    else
        log("Compact: enabling, but disable count still %d", disabled);
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

void SharedCompactor::compact(fragment_set const & fragments)
{
    log("Compact thread: compacting %d fragments", fragments.size());
    WallTimer timer;
    size_t outputCells = 0;
    size_t inputFragments = fragments.size();
    size_t inputSize = 0;            // active parts
    size_t restrictedFragments = 0;
    size_t restrictedSize = 0;

    // External correctness depends on the following properties, but
    // this function doesn't actually care:
    //   - fragments is topologically ordered oldest to newest
    //   - fragments must be in same locality group (e.g. 'table')


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

    range_vec rangeMap;
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
            CompactRangePtr p(new CompactRange((*t)->getRows()));

            // Find max-length adjacent sequence
            p->fragments = (*t)->getMaxAdjacentChain(fragments);

            // Unless the sequence includes at least two fragments,
            // there's no point in compacting it
            if(p->fragments.size() < 2)
                continue;

            // See if the sequence is rooted (doesn't have a parent)
            p->isRooted = !(fragDag.getParent(p->fragments.front(), *t));

            // Remember the sequence for the range
            rangeMap.push_back(p);
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
        fragDag.dumpDotGraph(out, fragments, false);
        dagLock.unlock();

        out.close();
    }
#endif

    // If we didn't find anything to compact, we're kind of hosed.
    if(rangeMap.empty())
        raise<ValueError>("fragment set contained no compactible sequences");


    // Organize the range map in ascending range order
    std::sort(rangeMap.begin(), rangeMap.end(), CompactRangePtrLt());

    // For each fragment in compaction set, remember:
    //   invRangeMap:
    //      fragment -> adjRange (ranges referencing fragment in
    //                            an adjacent sequence)

    typedef std::map<FragmentPtr, IntervalSet<string> > inv_range_map;

    // Invert rangeMap to build invRangeMap
    inv_range_map invRangeMap;
    for(range_vec::const_iterator i = rangeMap.begin();
        i != rangeMap.end(); ++i)
    {
        Interval<string> const & range = (*i)->range;
        fragment_vec const & adjSeq = (*i)->fragments;

        for(fragment_vec::const_iterator f = adjSeq.begin();
            f != adjSeq.end(); ++f)
        {
            invRangeMap[*f].add(range);
        }
    }

    log("Compact thread: starting compaction on table %s", table);


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

    typedef flux::Cutoff<Cell, IntervalPoint<string>, CellRowCutoff> CellCutoffStream;
    typedef CellCutoffStream::handle_t CellCutoffStreamPtr;
    typedef std::map<FragmentPtr, CellCutoffStreamPtr> input_map;
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

        CellCutoffStreamPtr p(new CellCutoffStream);
        p->pipeFrom(fragment->scan(pred));

        inputMap[fragment] = p;
    }

    // Prepare output
    CompactionOutput output(configMgr,
                            tracker,
                            dagMutex,
                            fragDag,
                            table,
                            fragments,
                            rangeMap);

    // Merge one range at a time
    for(range_vec::const_iterator i = rangeMap.begin();
        i != rangeMap.end(); ++i)
    {
        Interval<string> const & range = (*i)->range;
        fragment_vec const & adjSeq = (*i)->fragments;
        bool isRooted = (*i)->isRooted;

        log("Compact thread: compacting table %s, range: %s",
            table, ScanPredicate().setRowPredicate(IntervalSet<string>().add(range)));

        // Build a merge from all inputs involved
        CellStreamPtr merge = CellMerge::make(isRooted);
        for(fragment_vec::const_reverse_iterator f = adjSeq.rbegin();
            f != adjSeq.rend(); ++f)
        {
            // Get the input stream for the fragment
            CellCutoffStreamPtr & inputStream = inputMap[*f];

            // Advance limits on each active input
            inputStream->setCutoff(range.getUpperBound());

            // Add the input to the merge
            merge->pipeFrom(inputStream);
        }

        // Merge until we're out of input (i.e. done with the range)
        Cell x;
        while(merge->get(x))
        {
            output.put(x);
            ++outputCells;
        }
    }

    // We're done merging.  Flush output.
    output.flush();

    // Now that we're done, sweep the graph to get rid of old
    // fragments
    {
        lock_t dagLock(dagMutex);
        fragDag.sweepGraph();
    }

    // Report some stuff
    int64_t ms = timer.getElapsedNs() / 1000000;
    log("Compact stats: in %d %d rin %d %d out %d %d cells %d ms %d",
        inputFragments, inputSize,
        restrictedFragments, restrictedSize,
        output.getOutputFragmentCount(),
        output.getOutputSize(),
        outputCells, ms);

#ifdef COMPACTOR_DEBUG
    {
        ostringstream oss;
        oss << "compact." << compactionId << "." << table << ".B.txt";
        string fn = replace(oss.str(), "/", "_");
        ofstream out(fn.c_str());

        fragment_set moreFrags(fragments);
        moreFrags.insert(output.newFragments.begin(),
                         output.newFragments.end());

        lock_t dagLock(dagMutex);
        fragDag.dumpDotGraph(out, moreFrags, false);
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
        if(disabled)
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
