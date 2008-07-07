//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2006 Josh Taylor (Kosmix Corporation)
// Created 2006-01-24
// 
// This file is part of the warp library.
// 
// The warp library is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by the
// Free Software Foundation; either version 2 of the License, or any later
// version.
// 
// The warp library is distributed in the hope that it will be useful, but
// WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General
// Public License for more details.
// 
// You should have received a copy of the GNU General Public License along
// with this program; if not, write to the Free Software Foundation, Inc.,
// 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.
//----------------------------------------------------------------------------

#include <warp/gzip.h>

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
