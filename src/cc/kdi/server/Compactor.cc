//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-03-11
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

#include <kdi/server/Compactor.h>
#include <kdi/server/FragmentMerge.h>
#include <kdi/server/FragmentWriterFactory.h>
#include <kdi/server/FragmentWriter.h>
#include <kdi/server/TableSchema.h>
#include <warp/call_or_die.h>
#include <warp/log.h>
#include <warp/strutil.h>
#include <warp/timer.h>
#include <boost/scoped_ptr.hpp>
#include <cassert>

using namespace kdi::server;
using warp::log;
using warp::sizeString;

//----------------------------------------------------------------------------
// Compactor
//----------------------------------------------------------------------------
Compactor::Compactor(Input * input,
                     FragmentWriterFactory * writerFactory,
                     BlockCache * cache) :
    PersistentWorker("Compactor"),
    input(input),
    writerFactory(writerFactory),
    cache(cache)
{
}

bool Compactor::doWork()
{
    //log("Compactor: doWork()");

    boost::scoped_ptr<Work> work(input->getWork().release());
    if(!work)
    {
        //log("Compactor: no work");
        return false;
    }

    log("Compactor: compact");

    RangeOutputMap output;
    if(!compact(work->schema,
                work->groupIndex,
                work->compactionSet,
                output))
        return false;

    log("Compactor: done");

    work->done(output);

    //log("Compactor: complete");

    return true;
}

bool Compactor::compact(TableSchema const & schema, int groupIndex,
                        RangeFragmentMap const & compactionSet,
                        RangeOutputMap & outputSet) 
{
    assert(groupIndex >= 0 && (size_t)groupIndex < schema.groups.size());

    ScanPredicate groupPred = schema.groups[groupIndex].getPredicate();
    log("Compacting table %s group %s: %s", schema.tableName, groupIndex, groupPred);

    // Split outputs if they get bigger than this:
    size_t const OUTPUT_SPLIT_SIZE = size_t(512) << 20;  // 512 MB

    warp::WallTimer totTimer;
    size_t totSz = 0;
    size_t totCells = 0;

    warp::WallTimer outTimer;
    boost::scoped_ptr<FragmentWriter> writer;
    typedef std::vector<RangeFragmentMap::range_t> output_t;
    output_t outputRanges;

    RangeFragmentMap::const_iterator i;
    size_t nRanges = 0;
    for(i = compactionSet.begin(); i != compactionSet.end(); ++i)
    {
        ++nRanges;

        RangeFragmentMap::range_t const & range = i->first;
        RangeFragmentMap::frag_list_t const & frags = i->second;
        log("Compacting %d frags in range %d of %d: %s",
            frags.size(), nRanges, compactionSet.size(), range);

        // Open a new writer if we need one
        if(!writer)
            writer.reset(writerFactory->start(schema, groupIndex).release());

        // Remember the number of cells output so far
        size_t beforeCount = writer->getCellCount();

        // Merge everything in this range
        FragmentMerge merge(frags, cache, groupPred.clipRows(range), 0); 
        while(merge.copyMerged(size_t(-1), size_t(-1), *writer))
        {
            if(isCancelled())
                return false;
        }

        // See if we emitted any new cells for this range
        size_t cellsInRange = writer->getCellCount() - beforeCount;
        totCells += cellsInRange;

        if(cellsInRange)
        {
            // Put the new fragment in the replacement set
            outputRanges.push_back(range);
        }
        else
        {
            // Empty replacement set, this range has been compacted away
            outputSet[range] = "";
        }

        // If we've written enough, roll to a new file.
        size_t dataSz = writer->getDataSize();
        if(dataSz >= OUTPUT_SPLIT_SIZE)
        {
            double dt = outTimer.getElapsed();
            outTimer.reset();

            std::string fn = writer->finish();
            log("Compactor: wrote %s %sB in %.3f sec, %sB/s",
                fn, sizeString(dataSz), dt, sizeString(size_t(dataSz/dt)));
            writer.reset();

            for(output_t::const_iterator j = outputRanges.begin();
                j != outputRanges.end(); ++j)
            {
                outputSet[*j] = fn;
            }
            outputRanges.clear();

            totSz += dataSz;
        }
    }

    // Close last output
    if(writer)
    {
        size_t dataSz = writer->getDataSize();

        double dt = outTimer.getElapsed();
        outTimer.reset();

        std::string fn = writer->finish();
        log("Compactor: wrote %s %sB in %.3f sec, %sB/s",
            fn, sizeString(dataSz), dt, sizeString(size_t(dataSz/dt)));
        writer.reset();

        for(output_t::const_iterator j = outputRanges.begin();
            j != outputRanges.end(); ++j)
        {
            outputSet[*j] = fn;
        }
        outputRanges.clear();

        totSz += dataSz;
    }

    double dt = totTimer.getElapsed();
    log("Compaction complete for table %s, output %s cell(s), %sB in %.3f sec, %sB/s",
        schema.tableName, sizeString(totCells, 1000), sizeString(totSz),
        dt, sizeString(size_t(totSz/dt)));

    return true;
}

//----------------------------------------------------------------------------
// RangeFragmentMap
//----------------------------------------------------------------------------
RangeFragmentMap & RangeFragmentMap::operator=(RangeFragmentMap const  & x)
{
    rangeMap = x.rangeMap;
    return *this;
}

void RangeFragmentMap::addFragment(range_t range, FragmentCPtr const & f)
{
    map_t::iterator i = rangeMap.find(range);
    if(i == rangeMap.end())
    {
        frag_list_t frags;
        frags.push_back(f);
        rangeMap[range] = frags;
    }
    else
    {
        i->second.push_back(f);
    }
}

void RangeFragmentMap::addFragments(range_t range, frag_list_t const & frags) 
{
    for(frag_list_t::const_iterator i = frags.begin();
        i != frags.end(); ++i)
    {
        addFragment(range, *i);   
    }
}

