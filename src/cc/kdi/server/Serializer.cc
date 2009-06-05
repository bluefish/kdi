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

#include <kdi/server/Serializer.h>
#include <kdi/server/RowCollector.h>
#include <kdi/server/FragmentMerge.h>
#include <kdi/server/FragmentWriter.h>
#include <kdi/server/DirectBlockCache.h>
#include <kdi/scan_predicate.h>
#include <warp/timer.h>
#include <warp/log.h>
#include <warp/strutil.h>
#include <boost/scoped_ptr.hpp>

using namespace kdi::server;
using warp::log;
using warp::sizeString;

//----------------------------------------------------------------------------
// Serializer
//----------------------------------------------------------------------------
bool Serializer::doWork()
{
    //log("Serializer: doWork()");

    boost::scoped_ptr<Work> work(input->getWork().release());
    if(!work)
    {
        //log("Serializer: no work");
        return false;
    }

    log("Serializer: merging %d fragment(s) %s",
        work->fragments.size(), work->predicate);

    warp::WallTimer timer;

    DirectBlockCache cache;
    FragmentMerge merge(work->fragments, &cache, work->predicate, 0);

    std::vector<std::string> rows;
    RowCollector collector(rows, *work->output);

    while(merge.copyMerged(size_t(-1), size_t(-1), collector))
    {
        if(isCancelled())
            return false;
    }

    size_t outSz = work->output->getDataSize();
    float dt = timer.getElapsed();

    PendingFileCPtr outputFn = work->output->finish();

    log("Serializer: done, output %s, %d row(s), %sB, %.3f sec, %sB/s",
        outputFn->getName(), rows.size(), sizeString(outSz), dt,
        sizeString(size_t(outSz/dt)));

    work->done(rows, outputFn);

    //log("Serializer: complete");

    return true;
}
