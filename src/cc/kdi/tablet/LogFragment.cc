//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: kdi/tablet/LogFragment.cc $
//
// Created 2008/08/11
//
// Copyright 2008 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#include <kdi/tablet/LogFragment.h>
#include <kdi/memory_table.h>
#include <kdi/synchronized_table.h>

using namespace kdi;
using namespace kdi::tablet;

namespace
{
    template <class T>
    class EmptyScan : public flux::Stream<T>
    {
    public:
        bool get(T & x) { return false; }
    };
}

//----------------------------------------------------------------------------
// LogFragment
//----------------------------------------------------------------------------
LogFragment::LogFragment(std::string const & uri) :
    logTable(
        SynchronizedTable::make(
            MemoryTable::create(false)
            )->makeBuffer()
        ),
    uri(uri)
{
}

CellStreamPtr LogFragment::scan(ScanPredicate const & pred) const
{
    return logTable->scan(pred);
}

bool LogFragment::isImmutable() const
{
    return false;
}

std::string LogFragment::getFragmentUri() const
{
    return uri;
}

size_t LogFragment::getDiskSize(warp::Interval<std::string> const & rows) const
{
    return 0;
}

flux::Stream< std::pair<std::string, size_t> >::handle_t
LogFragment::scanIndex(warp::Interval<std::string> const & rows) const
{
    flux::Stream< std::pair<std::string, size_t> >::handle_t p(
        new EmptyScan<std::pair<std::string, size_t> >
        );
    return p;
}
