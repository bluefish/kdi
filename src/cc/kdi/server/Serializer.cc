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
#include <boost/scoped_ptr.hpp>

using namespace kdi::server;

//----------------------------------------------------------------------------
// Serializer
//----------------------------------------------------------------------------
bool Serializer::doWork()
{
    boost::scoped_ptr<Work> work(input->getWork().release());
    if(!work)
        return false;

    DirectBlockCache cache;
    FragmentMerge merge(work->fragments, &cache, work->predicate, 0);

    std::vector<std::string> rows;
    RowCollector collector(rows, *work->output);

    while(merge.copyMerged(size_t(-1), size_t(-1), collector))
    {
        if(isCancelled())
            return false;
    }

    std::string outputFn = work->output->finish();
    work->done(rows, outputFn);

    return true;
}
