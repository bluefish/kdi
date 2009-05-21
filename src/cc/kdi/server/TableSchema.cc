//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-04-30
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

#include <kdi/server/TableSchema.h>
#include <kdi/server/misc_util.h>
#include <kdi/scan_predicate.h>
#include <kdi/timestamp.h>
#include <warp/interval.h>

using namespace kdi::server;

//----------------------------------------------------------------------------
// TableSchema
//----------------------------------------------------------------------------
kdi::ScanPredicate TableSchema::Group::getPredicate() const
{
    ScanPredicate p;

    p.setColumnPredicate(getFamilyColumnSet(families));

    if(maxAge > 0)
    {
        p.setTimePredicate(
            warp::IntervalSet<int64_t>()
            .add(warp::makeLowerBound(
                     Timestamp::now().toMicroseconds() - maxAge))
            );
    }

    if(maxHistory > 0)
    {
        p.setMaxHistory(maxHistory);
    }

    return p;
}
