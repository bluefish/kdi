//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-01-15
//
// This file is part of KDI.
// 
// KDI is free software; you can redistribute it and/or modify it
// under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// any later version.
// 
// KDI is distributed in the hope that it will be useful, but WITHOUT
// ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
// or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public
// License for more details.
// 
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
// 02110-1301, USA.
//----------------------------------------------------------------------------

#include <warp/perfitem.h>
#include <warp/perflog.h>

using namespace warp;


//----------------------------------------------------------------------------
// PerformanceItem
//----------------------------------------------------------------------------
PerformanceItem::PerformanceItem() :
    tracker(0), trackerIndex(0), inTeardown(false)
{
}

PerformanceItem::~PerformanceItem()
{
    try {
        inTeardown = true;
        if(tracker)
            tracker->removeItem(this);
    }
    catch(...) {
        // No exceptions...
    }
}

bool PerformanceItem::isTracked() const
{
    return tracker != 0;
}

void PerformanceItem::removeSelf()
{
    if(tracker)
        tracker->removeItem(this);
}
