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

#ifndef WARP_TICKS_H
#define WARP_TICKS_H

#include <stdint.h>

namespace warp {

    /// Get value of hardware cycle counter
    inline uint64_t getTicks()
    {
        unsigned a, d;
        asm volatile("rdtsc" : "=a" (a), "=d" (d));
        return uint64_t(a) | (uint64_t(d) << 32);
    }

    /// Get estimate of hardware tick rate (CPU frequency) in
    /// cycles/sec.  This function blocks for a while and estimates
    /// from that.  It's not fast, so don't call it in
    /// performance-critical code.  Call it once and remember the
    /// result.
    uint64_t estimateTickRate();

} // namespace warp

#endif // WARP_TICKS_H
