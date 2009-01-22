//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
// Created 2008-12-10
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

#include <kdi/tablet/Fragment.h>

using namespace kdi::tablet;
using namespace warp;
using namespace std;

size_t Fragment::getDiskSize(IntervalSet<string> const & rows) const
{
    size_t sz = 0;
    for(IntervalSet<string>::const_iterator i = rows.begin();
        i != rows.end();)
    {
        IntervalPoint<string> const & lo = *i;  ++i;
        IntervalPoint<string> const & hi = *i;  ++i;

        sz += getDiskSize(Interval<string>(lo, hi));
    }

    return sz;
}

size_t Fragment::getDiskSize() const {
    Interval<string> unbounded = makeUnboundedInterval<string>();
    return getDiskSize(unbounded);
}

