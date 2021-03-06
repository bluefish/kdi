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
#include <kdi/tablet/FileTracker.h>
#include <kdi/tablet/Fragment.h>
#include <kdi/cell_merge.h>
#include <kdi/scan_predicate.h>
#include <flux/cutoff.h>
#include <flux/threaded_reader.h>
#include <warp/functional.h>
#include <warp/uri.h>
#include <warp/log.h>
#include <warp/call_or_die.h>
#include <warp/timer.h>
#include <warp/WorkerPool.h>
#include <ex/exception.h>
#include <boost/bind.hpp>
#include <iostream>
#include <vector>

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

    // Stop the compaction entirely if it gets bigger than this:
    size_t const MAX_OUTPUT_SIZE = 4 * OUTPUT_SPLIT_SIZE;

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

    class CompactRangePtrLt
    {
        IntervalPoint<string> const & get(CompactRangePtr const & x) const
        {
            return x->range.getUpperBound();
        }

        IntervalPoint<string> const & get(IntervalPoint<string> const & x) const
        {
            return x;
        }

    public:
        template <class A, class B>
        bool operator()(A const & a, B const & b) const
        {
            return get(a) < get(b);
        }
    };

    typedef std::vector<CompactRangePtr> range_vec;

    struct SizeOfCell
    {
        size_t operator()(Cell const & x) const
        {
            return sizeof(Cell) + x.getRow().size() + x.getColumn().size()
                + x.getValue().size();
        }
    };

}

//----------------------------------------------------------------------------
// ReadAheadImpl
//----------------------------------------------------------------------------
class SharedCompactor::ReadAheadImpl
{
    boost::scoped_ptr<warp::WorkerPool> pool;
    size_t readAhead;
    size_t readUpTo;

public:
    ReadAheadImpl() :
        readAhead(4 << 20),
        readUpTo(512 << 10)
        
    {
        if(char * s = getenv("KDI_TABLET_READ_AHEAD"))
            readAhead = parseSize(s);

        if(char * s = getenv("KDI_TABLET_READ_UP_TO"))
            readUpTo = parseSize(s);

        size_t nThreads = 4;
        if(char * s = getenv("KDI_TABLET_READ_THREADS"))
            nThreads = parseSize(s);

        if(readAhead && readUpTo && nThreads)
        {
            log("Compaction read-ahead: nThreads=%d, readAhead=%s, "
                "readUpTo=%s", nThreads, sizeString(readAhead),
                sizeString(readUpTo));

            pool.reset(
                new WorkerPool(
                    nThreads,
                    "Compaction read-ahead thread",
                    true)
                );
        }
        else
        {
            log("Compaction read-ahead: disabled");
        }
    }

    ~ReadAheadImpl() {}

    CellStreamPtr wrap(CellStreamPtr const & base) const
    {
        if(!pool)
            return base;

        CellStreamPtr p = flux::makeThreadedReader<Cell>(
            *pool, readAhead, readUpTo, SizeOfCell());

        p->pipeFrom(base);
        return p;
    }
};



//----------------------------------------------------------------------------
// CompactionOutput
//----------------------------------------------------------------------------
namespace {

    class CompactionOutput
    {
        typedef boost::mutex::scoped_lock lock_t;

        FragmentLoader * loader;
        FragmentWriter * writer;

        FileTrackerPtr tracker;
        boost::mutex & dagMutex;
        FragDag & fragDag;
        string table;
        fragment_set const & fragments;
        range_vec const & rangeMap;

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
        //   Instead of having the fragment set, we might want some
        //   kind of SplitFinder interface encapsulating the mutex,
        //   fragDag, and fragment set.
        //
        //   Could have a replacement callback when compaction is
        //   done, which would encapsulate the tracker, dagMutex, and
        //   fragDag.

        CompactionOutput(FragmentLoader * loader,
                         FragmentWriter * writer,
                         FileTrackerPtr const & tracker,
                         boost::mutex & dagMutex,
                         FragDag & fragDag,
                         string const & table,
                         fragment_set const & fragments,
                         range_vec const & rangeMap
            ) :
            loader(loader),
            writer(writer),
            tracker(tracker),
            dagMutex(dagMutex),
            fragDag(fragDag),
            table(table),
            fragments(fragments),
            rangeMap(rangeMap),
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
                    sizeString(writer->size()),
                    reprString(outputRange.getUpperBound().getValue()));

                finalizeOutput();
            }

            // If we don't have an active output, open a new one
            if(!outputOpen)
            {
                // Start an output file for the compacting table
                writer->start(table);
                outputOpen = true;
            }

            // Add another cell to the output
            writer->put(x);

            // Check to see if the output is large enough to split if
            // haven't already chosen one.
            if(!readyToSplit && writer->size() > OUTPUT_SPLIT_SIZE)
            {
                // Choose a split point on a tablet boundary and restrict
                // outputRange.  The last row we added to the output forms
                // an inclusive lower bound for the split point.  We'll
                // split the first time we see a cell outside outputRange.
                IntervalPoint<string> curRow(x.getRow().toString(), PT_POINT);

                // Find the first range that contains curRow
                range_vec::const_iterator i = std::lower_bound(
                    rangeMap.begin(), rangeMap.end(), curRow,
                    CompactRangePtrLt());

                // Set the output upper bound to the range upper bound
                if(i != rangeMap.end())
                    outputRange.setUpperBound((*i)->range.getUpperBound());
                else
                    outputRange.unsetUpperBound();

                // Set the split flag
                readyToSplit = true;
            }
        }

        void flush(IntervalPoint<string> & outputUpperBound)
        {
            if(outputOpen)
            {
                // If not everything in the range map was processed
                // need to be told not to reassign any tablets
                // we didn't get to
                outputRange.setUpperBound(outputUpperBound);
                log("Compact thread: last output upper bound = %s)", outputUpperBound);

                log("Compact thread: last output (sz=%s)", sizeString(writer->size()));
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
            outputSize += writer->size();
            string uri = writer->finish();
            ++nFragments;
            outputOpen = false;

            // Open the newly written fragment
            FragmentPtr frag = loader->load(uri);

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
SharedCompactor::SharedCompactor(FragmentLoader * loader,
                                 FragmentWriter * writer,
                                 warp::StatTracker * statTracker) :
    loader(loader),
    writer(writer),
    statTracker(statTracker),
    disabled(0),
    cancel(false),
    fragDag(statTracker)
{
    EX_CHECK_NULL(loader);
    EX_CHECK_NULL(writer);

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

namespace {
    class Compaction {
        typedef vector<CompactionList> cvec;
        cvec const & tablet_lists;

    public:
        Compaction(cvec const & tablet_lists) :
            tablet_lists(tablet_lists)
        {
        }

        fragment_set getAllFragments() {
            fragment_set all_fragments;
            for(cvec::const_iterator i = tablet_lists.begin();
                i != tablet_lists.end(); ++i) 
            {
                fragment_vec const & f = (*i).fragments;
                all_fragments.insert(f.begin(), f.end());
            }
            return all_fragments;
        }
    };
}

void SharedCompactor::compact(vector<CompactionList> const & compactions)
{
    Compaction c(compactions);
    fragment_set fragments = c.getAllFragments();
    
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
    FileTrackerPtr tracker;
    string table;

    // Pull our random parameters from one of the tablets.  It 
    // doesn't matter which as all of the tablets should have the 
    // same information.
    if(!compactions.empty())
    {
        Tablet const * t = compactions.front().tablet;
        tracker = t->getFileTracker();
        table = t->getTableName();
    }

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

        for(vector<CompactionList>::const_iterator i = compactions.begin();
            i != compactions.end(); ++i) 
        {
            Tablet const * t = (*i).tablet;
            CompactRangePtr p(new CompactRange(t->getRows()));
            p->fragments = (*i).fragments;
            p->isRooted = !(fragDag.getParent(p->fragments.front(), t));
            rangeMap.push_back(p);
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

    if(!readAhead)
        readAhead.reset(new ReadAheadImpl());

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

        CellCutoffStreamPtr cutoff(new CellCutoffStream);
        cutoff->pipeFrom(
            readAhead->wrap(
                fragment->scan(pred)
                )
            );

        inputMap[fragment] = cutoff;
    }

    // Prepare output
    CompactionOutput output(loader,
                            writer,
                            tracker,
                            dagMutex,
                            fragDag,
                            table,
                            fragments,
                            rangeMap);

    // Merge one range at a time
    range_vec::const_iterator i;
    for(i = rangeMap.begin();
        i != rangeMap.end() && output.getOutputSize() < MAX_OUTPUT_SIZE; 
        ++i)
    {
        {
            lock_t lock(mutex);
            if(cancel)
            {
                log("Compact thread: cancelled");
                return;
            }
        }

        Interval<string> const & range = (*i)->range;
        fragment_vec const & adjSeq = (*i)->fragments;
        bool filterErasures = (*i)->isRooted;

        log("Compact thread: compacting table %s, %srange: %s",
            table,
            (filterErasures ? "rooted " : ""),
            ScanPredicate().setRowPredicate(IntervalSet<string>().add(range)));

        // Build a merge from all inputs involved
        CellStreamPtr merge = CellMerge::make(filterErasures);
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
        size_t outputBegin = outputCells;
        Cell x;
        while(merge->get(x))
        {
            output.put(x);
            ++outputCells;
        }

        // Did we have any output?
        if(outputBegin == outputCells)
        {
            // This section had no output cells -- remove the
            // fragments from the tablet chain(s)
            lock_t dagLock(dagMutex);
            fragDag.removeFragments(range, adjSeq);
        }
    }

    IntervalPoint<string> outputUpperBound(string(), PT_INFINITE_UPPER_BOUND);
    if(i < rangeMap.end()) {
        outputUpperBound = (*i)->range.getLowerBound().getAdjacentComplement();
    }

    // We're done merging.  Flush output.
    output.flush(outputUpperBound);

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
        lock.unlock();
        log("Compact thread: choosing compaction set");
        lock_t dagLock(dagMutex);
        vector<CompactionList> compactions = fragDag.chooseCompactionSet();
        dagLock.unlock();
        lock.lock();
        if(cancel || disabled)
            continue;

        if(compactions.empty())
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
            compact(compactions);
            lock.lock();
        }
    }
}
