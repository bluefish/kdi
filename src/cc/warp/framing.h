//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2006 Josh Taylor (Kosmix Corporation)
// Created 2006-10-26
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

#ifndef WARP_FRAMING_H
#define WARP_FRAMING_H

#include <warp/rollinghash.h>

namespace warp
{
    template <class RHash, class It>
    It findFrameHeader(It begin, It end, RHash & computedHash)
    {
        // Assumes LSB integer serialization

        typedef RollingHash<modulus_t, window_t> rolling_hash_t;
        typedef typename rolling_hash_t::integer_t integer_t;

        size_t const SHIFT = sizeof(integer_t) * 8 - 8;
        size_t const HEADER_SZ = sizeof(header_t);
        size_t const CHECK_SZ = sizeof(integer_t);

        rolling_hash_t computedHash;
        integer_t inputHash;

        // Initialize first frame
        if(end - begin < ssize_t(HEADER_SZ + CHECK_SZ))
            return end;

        // Read frame header and compute hash
        for(size_t i = 0; i < HEADER_SZ; ++i)
            computedHash.update(*begin++);

        // Read header hash
        char * p = reinterpret_cast<char *>(&inputHash);
        for(size_t i = 0; i < CHECK_SZ; ++i)
            *p++ = *begin++;

        // Compare initial frame
        if(computedHash.getHash() == inputHash)
            return begin - HEADER_SZ - CHECK_SZ;

        // Scan for a valid frame
        while(begin != end)
        {
            // Move next byte of inputHash into header window
            computedHash.update(inputHash & 0xff);

            // Move next byte of input into inputHash
            inputHash = (inputHash >> 8) | (integer_t(*begin++) << SHIFT);

            // Compare
            if(computedHash.getHash() == inputHash)
                return begin - HEADER_SZ - CHECK_SZ;
        }

        // No go
        return end;
    }
}

#endif // WARP_FRAMING_H
