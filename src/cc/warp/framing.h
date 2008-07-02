//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/warp/framing.h#1 $
//
// Created 2006/10/26
//
// Copyright 2006 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef WARP_FRAMING_H
#define WARP_FRAMING_H

#include "rollinghash.h"

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
