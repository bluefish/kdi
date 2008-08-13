//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: kdi/tablet/DiskFragment.cc $
//
// Created 2008/08/07
//
// Copyright 2008 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#include <kdi/tablet/DiskFragment.h>
#include <warp/uri.h>

using namespace kdi;
using namespace kdi::tablet;
using namespace warp;

//----------------------------------------------------------------------------
// DiskFragment
//----------------------------------------------------------------------------
DiskFragment::DiskFragment(std::string const & uri) :
    uri(uri),
    table(uriPushScheme(uriPopScheme(uri), "cache"))
{
}


CellStreamPtr DiskFragment::scan(ScanPredicate const & pred) const
{
    return table.scan(pred);
}

bool DiskFragment::isImmutable() const
{
    return true;
}

std::string DiskFragment::getFragmentUri() const
{
    return uri;
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
    return table.scanIndex(rows);
}
