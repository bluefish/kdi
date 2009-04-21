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

#include <boost/bind.hpp>
#include <kdi/server/Compactor.h>
#include <kdi/server/FragmentMerge.h>
#include <kdi/server/TabletEventListener.h>
#include <warp/call_or_die.h>
#include <warp/fs.h>
#include <warp/log.h>

using namespace kdi;
using namespace kdi::server;
using std::string;
using std::vector;
using std::map;
using std::search;
using warp::Interval;
using warp::log;
using warp::File;

Compactor::Compactor(BlockCache * cache) :
    cache(cache),
    writer(64 << 10)
{
    EX_CHECK_NULL(cache);

    thread.reset(
        new boost::thread(
            warp::callOrDie(
                boost::bind(
                    &Compactor::compactLoop,
                    this
                ),
                "Compact thread", true
            )));

    log("Compactor %p: created", this);
}

void Compactor::compact(RangeFragmentMap const & compactionSet,
                        DiskFragmentMaker * fragMaker,
                        RangeFragmentMap & outputSet) 
{
    // Split outputs if they get bigger than this:
    size_t const OUTPUT_SPLIT_SIZE = size_t(1) << 30;  // 1 GB

    // Stop the compaction entirely if it gets bigger than this:
    size_t const MAX_OUTPUT_SIZE = 4 * OUTPUT_SPLIT_SIZE;

    writer.open(fragMaker->newDiskFragment());

    RangeFragmentMap::const_iterator i;
    for(i = compactionSet.begin(); i != compactionSet.end(); ++i)
    {
        if(writer.getDataSize() > MAX_OUTPUT_SIZE)
        {
            writer.close();
            writer.open(fragMaker->newDiskFragment());
        }

        RangeFragmentMap::range_t const & range = i->first;
        RangeFragmentMap::frag_list_t const & frags = i->second;

        log("compacting %d frags in range %s", frags.size(), range);

        ScanPredicate rangePred("");
        rangePred.clipRows(range);
        FragmentMerge merge(frags, cache, rangePred, 0); 

        const size_t maxCells = 10000000;
        const size_t maxSize= 10000000;
        size_t outputSize = writer.getDataSize();
        while(merge.copyMerged(maxCells, maxSize, writer));
        outputSize = writer.getDataSize()-outputSize;
            
        if(outputSize > 0)
        {
            // Put the new fragment in the replacement set
        }
        else
        {
            // Empty replacement set, this range has been compacted away
        }
    }

    log("compaction output, cells: %d, size: %d", writer.getCellCount(), writer.getDataSize());

    writer.close();
}

void Compactor::chooseCompactionSet(RangeFragmentMap const & fragMap,
                                    RangeFragmentMap & compactionSet)
{
    compactionSet.clear();
    compactionSet = fragMap;
}

void Compactor::compactLoop() 
{
    log("Compactor: starting");
}

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
