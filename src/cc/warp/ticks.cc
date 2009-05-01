//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-04-30
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

#include <warp/ticks.h>
#include <sys/time.h>
#include <unistd.h>

uint64_t warp::estimateTickRate()
{
    struct timeval t1, t2;
    uint64_t n1, n2;

    gettimeofday(&t1, 0);
    n1 = getTicks();
        
    usleep(200000);
        
    gettimeofday(&t2, 0);
    n2 = getTicks();

    uint64_t d_usec = (t2.tv_sec * 1000000 + t2.tv_usec) -
        (t1.tv_sec * 1000000 + t1.tv_usec);
        
    return (n2 - n1) * 1000000 / d_usec;
}
