//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
// Created 2008-08-07
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

#include <kdi/tablet/DiskFragment.h>
#include <warp/uri.h>
#include <warp/StatTracker.h>

using namespace kdi;
using namespace kdi::tablet;
using namespace warp;

//----------------------------------------------------------------------------
// DiskFragment
//----------------------------------------------------------------------------
DiskFragment::DiskFragment(std::string const & uri,
                           warp::StatTracker * tracker) :
    uri(uri),
    table(
        kdi::local::DiskTable::loadTable(
            uriPushScheme(uriPopScheme(uri), "cache"))),
    tracker(tracker)
{
    EX_CHECK_NULL(tracker);

    tracker->add("DiskFragment.indexSize", table->getIndexSize());
    tracker->add("DiskFragment.dataSize", table->getDataSize());
    tracker->add("DiskFragment.nActive", 1);
    tracker->add("DiskFragment.nTotal", 1);
}

DiskFragment::~DiskFragment()
{
    tracker->add("DiskFragment.indexSize", -int64_t(table->getIndexSize()));
    tracker->add("DiskFragment.dataSize", -int64_t(table->getDataSize()));
    tracker->add("DiskFragment.nActive", -1);
}

CellStreamPtr DiskFragment::scan(ScanPredicate const & pred) const
{
    return table->scan(pred);
}

bool DiskFragment::isImmutable() const
{
    return true;
}

std::string DiskFragment::getFragmentUri() const
{
    return uri;
}

std::string DiskFragment::getDiskUri() const
{
    return uriPopScheme(uri);
}

size_t DiskFragment::getDiskSize(warp::Interval<std::string> const & rows) const
{
    flux::Stream< std::pair<std::string, size_t> >::handle_t stream =
        scanIndex(rows);

    std::pair<std::string, size_t> x;
    size_t total = 0;
    while(stream->get(x))
        total += x.second;

    return total;
}

flux::Stream< std::pair<std::string, size_t> >::handle_t
DiskFragment::scanIndex(warp::Interval<std::string> const & rows) const
{
    return table->scanIndex(rows);
}
