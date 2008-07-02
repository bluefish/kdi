//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/warp/perfitem.cc#1 $
//
// Created 2007/01/15
//
// Copyright 2007 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#include "perfitem.h"
#include "perflog.h"

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
