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

namespace {

std::string getUniqueTableFile(std::string const & rootDir,
                               std::string const & tableName)
{
    string dir = warp::fs::resolve(rootDir, tableName);

    // XXX: this should be cached -- only need to make the directory
    // once per table
    warp::fs::makedirs(dir);
   
    return File::openUnique(warp::fs::resolve(dir, "$UNIQUE")).second;
}

}

void Compactor::compact(RangeFragmentMap const & compactionSet,
                        RangeFragmentMap & outputSet) 
{
    string outputName = getUniqueTableFile("memfs:/", "test");
    writer.open(outputName);

    RangeFragmentMap::const_iterator i;
    for(i = compactionSet.begin(); i != compactionSet.end(); ++i)
    {
        RangeFragmentMap::range_t const & range = i->first;
        RangeFragmentMap::frag_list_t const & frags = i->second;

        log("compacting %d frags in range %s", frags.size(), range);

        FragmentMerge merge(frags, cache, ScanPredicate(""), 0); 
        const size_t maxCells = 10000000;
        const size_t maxSize= 10000000;

        log("doing compaction merge");
        bool done = merge.copyMerged(maxCells, maxSize, writer);
        if(done) log("compaction merge finished");
        log("cells: %d, size: %d", maxCells, maxSize);
    }

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

void RangeFragmentMap::addFragment(range_t range, Fragment const * f)
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
