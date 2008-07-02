//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/warp/init.cc#2 $
//
// Created 2006/08/13
//
// Copyright 2006 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#include <warp/init.h>

void warp::initModule()
{
    static bool once = false;
    if(!once)
    {
        once = true;

        WARP_CALL_INIT(warp_dir);
        WARP_CALL_INIT(warp_file);
        WARP_CALL_INIT(warp_file_cache);
        WARP_CALL_INIT(warp_fs);
        WARP_CALL_INIT(warp_memfs);
        WARP_CALL_INIT(warp_mmapfile);
        WARP_CALL_INIT(warp_perflog);
        WARP_CALL_INIT(warp_perfrusage);
        WARP_CALL_INIT(warp_posixdir);
        WARP_CALL_INIT(warp_posixfs);
        WARP_CALL_INIT(warp_stdfile);
    }
}
