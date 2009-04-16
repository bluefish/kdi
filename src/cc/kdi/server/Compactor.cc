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
#include <warp/log.h>

using namespace kdi;
using namespace kdi::server;
using std::string;
using std::vector;
using std::map;
using std::search;
using warp::Interval;
using warp::log;

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
            )
        )
    );

    log("Compactor %p: created", this);
}

void Compactor::compact(RangeFragmentMap const & compactionSet,
                        RangeFragmentMap & outputSet) 
{
}

void Compactor::chooseCompactionSet(RangeFragmentMap const & fragMap,
                                    RangeFragmentMap & compactionSet)
{
}

void Compactor::compactLoop() 
{
    log("Compactor: starting");
}
