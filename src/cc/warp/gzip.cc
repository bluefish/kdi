//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/warp/gzip.cc#1 $
//
// Created 2006/01/24
//
// Copyright 2006 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#include "gzip.h"

using namespace warp;
using namespace ex;

//----------------------------------------------------------------------------
// raiseZlibError
//----------------------------------------------------------------------------
void warp::raiseZlibError(int zErr)
{
    switch(zErr)
    {
        case Z_NEED_DICT:     raise<ZlibError>("need dictionary");
        case Z_DATA_ERROR:    raise<ZlibError>("data error");
        case Z_MEM_ERROR:     raise<ZlibError>("memory error");
        case Z_VERSION_ERROR: raise<ZlibError>("version error: lib=%s, hdr=%s",
                                               ::zlibVersion(), ZLIB_VERSION);
        case Z_STREAM_ERROR:  raise<ZlibError>("stream error");
        default:              raise<ZlibError>("unknown error: %d", zErr);
    }
}
