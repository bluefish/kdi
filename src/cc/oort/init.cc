//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/oort/init.cc#1 $
//
// Created 2006/08/18
//
// Copyright 2006 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#include "init.h"
#include "warp/init.h"

void oort::initModule()
{
    static bool once = false;
    if(!once)
    {
        once = true;
        
        warp::initModule();

        WARP_CALL_INIT(oort_recordstream);
        WARP_CALL_INIT(oort_splitstream);
        WARP_CALL_INIT(oort_checkstream);
    }
}
