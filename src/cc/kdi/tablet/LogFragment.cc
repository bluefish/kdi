//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
// Created 2008-08-11
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
