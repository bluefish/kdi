//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-03-06
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

#ifndef WARP_VINT_H
#define WARP_VINT_H

#include <stddef.h>
#include <stdint.h>

namespace warp {
namespace vint {

    inline size_t getEncodedLength(uint64_t x)
    {
        if(x < 128u) return 1;
        if(x < 16384u) return 2;
        if(x < 2097152u) return 3;
        if(x < 268435456u) return 4;
        if(x < 34359738368u) return 5;
        if(x < 4398046511104u) return 6;
        if(x < 562949953421312u) return 7;
        if(x < 72057594037927936u) return 8;
        return 9;
    }

    inline size_t getEncodedLength(uint32_t x)
    {
        if(x < 128u) return 1;
        if(x < 16384u) return 2;
        if(x < 2097152u) return 3;
        if(x < 268435456u) return 4;
        return 5;
    }

    inline size_t decode(void const * src, uint64_t & x)
    {
        uint8_t const * p = static_cast<uint8_t const *>(src);
            
        x = p[0] & 0x7f;
        if((p[0] & 0x80) == 0) return 1;

        x |= uint64_t(p[1] & 0x7f) << 7;
        if((p[1] & 0x80) == 0) return 2;

        x |= uint64_t(p[2] & 0x7f) << 14;
        if((p[2] & 0x80) == 0) return 3;

        x |= uint64_t(p[3] & 0x7f) << 21;
        if((p[3] & 0x80) == 0) return 4;

        x |= uint64_t(p[4] & 0x7f) << 28;
        if((p[4] & 0x80) == 0) return 5;

        x |= uint64_t(p[5] & 0x7f) << 35;
        if((p[5] & 0x80) == 0) return 6;

        x |= uint64_t(p[6] & 0x7f) << 42;
        if((p[6] & 0x80) == 0) return 7;

        x |= uint64_t(p[7] & 0x7f) << 49;
        if((p[7] & 0x80) == 0) return 8;

        x |= uint64_t(p[8]) << 56;
        return 9;
    }

    inline size_t decode(void const * src, uint32_t & x)
    {
        uint8_t const * p = static_cast<uint8_t const *>(src);
            
        x = p[0] & 0x7f;
        if((p[0] & 0x80) == 0) return 1;

        x |= uint32_t(p[1] & 0x7f) << 7;
        if((p[1] & 0x80) == 0) return 2;

        x |= uint32_t(p[2] & 0x7f) << 14;
        if((p[2] & 0x80) == 0) return 3;

        x |= uint32_t(p[3] & 0x7f) << 21;
        if((p[3] & 0x80) == 0) return 4;

        x |= uint32_t(p[4] & 0x0f) << 28;
        return 5;
    }

    inline size_t encode(uint64_t x, void * dst)
    {
        uint8_t * p = static_cast<uint8_t *>(dst);
            
        if(x < 128u) {
            p[0] = x & 0x7f;
            return 1;
        }
        p[0] = (x & 0x7f) | 0x80;

        if(x < 16384u) {
            p[1] = (x >> 7) & 0x7f;
            return 2;
        }
        p[1] = ((x >> 7) & 0x7f) | 0x80;

        if(x < 2097152u) {
            p[2] = (x >> 14) & 0x7f;
            return 3;
        }
        p[2] = ((x >> 14) & 0x7f) | 0x80;

        if(x < 268435456u) {
            p[3] = (x >> 21) & 0x7f;
            return 4;
        }
        p[3] = ((x >> 21) & 0x7f) | 0x80;

        if(x < 34359738368u) {
            p[4] = (x >> 28) & 0x7f;
            return 5;
        }
        p[4] = ((x >> 28) & 0x7f) | 0x80;

        if(x < 4398046511104u) {
            p[5] = (x >> 35) & 0x7f;
            return 6;
        }
        p[5] = ((x >> 35) & 0x7f) | 0x80;

        if(x < 562949953421312u) {
            p[6] = (x >> 42) & 0x7f;
            return 7;
        }
        p[6] = ((x >> 42) & 0x7f) | 0x80;

        if(x < 72057594037927936u) {
            p[7] = (x >> 49) & 0x7f;
            return 8;
        }
        p[7] = ((x >> 49) & 0x7f) | 0x80;

        p[8] = (x >> 56) & 0xff;
        return 9;
    }

    inline size_t encode(uint32_t x, void * dst)
    {
        uint8_t * p = static_cast<uint8_t *>(dst);
            
        if(x < 128u) {
            p[0] = x & 0x7f;
            return 1;
        }
        p[0] = (x & 0x7f) | 0x80;

        if(x < 16384u) {
            p[1] = (x >> 7) & 0x7f;
            return 2;
        }
        p[1] = ((x >> 7) & 0x7f) | 0x80;

        if(x < 2097152u) {
            p[2] = (x >> 14) & 0x7f;
            return 3;
        }
        p[2] = ((x >> 14) & 0x7f) | 0x80;

        if(x < 268435456u) {
            p[3] = (x >> 21) & 0x7f;
            return 4;
        }
        p[3] = ((x >> 21) & 0x7f) | 0x80;

        p[4] = (x >> 28) & 0x0f;
        return 5;
    }

    struct QuadInfo
    {
        uint8_t dataLen;
        uint8_t offset[15];
    };
    extern QuadInfo const QUAD_INFO[256];

        
    inline size_t decodeQuad(void const * src, uint32_t * x)
    {
        uint8_t const * p = static_cast<uint8_t const *>(src);
        uint8_t * d = reinterpret_cast<uint8_t *>(x);
            
        QuadInfo const & q = QUAD_INFO[p[0]];

        x[0] = 0;
        x[1] = 0;
        x[2] = 0;
        x[3] = 0;
            
        ++p;
        switch(q.dataLen)
        {
            case 16: d[q.offset[14]] = p[15];
            case 15: d[q.offset[13]] = p[14];
            case 14: d[q.offset[12]] = p[13];
            case 13: d[q.offset[11]] = p[12];
            case 12: d[q.offset[10]] = p[11];
            case 11: d[q.offset[9]]  = p[10];
            case 10: d[q.offset[8]]  = p[9];
            case 9:  d[q.offset[7]]  = p[8];
            case 8:  d[q.offset[6]]  = p[7];
            case 7:  d[q.offset[5]]  = p[6];
            case 6:  d[q.offset[4]]  = p[5];
            case 5:  d[q.offset[3]]  = p[4];
            case 4:  d[q.offset[2]]  = p[3];
                     d[q.offset[1]]  = p[2];
                     d[q.offset[0]]  = p[1];
                     d[0]            = p[0];
        }
        return q.dataLen+1;
    }

    inline uint8_t getByteLength(uint32_t x)
    {
        if(x < 256u) return 1;
        if(x < 65536u) return 2;
        if(x < 16777216u) return 3;
        return 4;
    }

    inline size_t encodeQuad(uint32_t const * x, void * dst)
    {
        uint8_t const * s = reinterpret_cast<uint8_t const *>(x);
        uint8_t * p = static_cast<uint8_t *>(dst);

        p[0] = ( ((getByteLength(x[0]) - 1) << 6) |
                 ((getByteLength(x[1]) - 1) << 4) |
                 ((getByteLength(x[2]) - 1) << 2) |
                 ((getByteLength(x[3]) - 1)     ) );

        QuadInfo const & q = QUAD_INFO[p[0]];
            
        ++p;
        switch(q.dataLen)
        {
            case 16: p[15] = s[q.offset[14]];
            case 15: p[14] = s[q.offset[13]];
            case 14: p[13] = s[q.offset[12]];
            case 13: p[12] = s[q.offset[11]];
            case 12: p[11] = s[q.offset[10]];
            case 11: p[10] = s[q.offset[9]];
            case 10: p[9]  = s[q.offset[8]];
            case 9:  p[8]  = s[q.offset[7]];
            case 8:  p[7]  = s[q.offset[6]];
            case 7:  p[6]  = s[q.offset[5]];
            case 6:  p[5]  = s[q.offset[4]];
            case 5:  p[4]  = s[q.offset[3]];
            case 4:  p[3]  = s[q.offset[2]];
                     p[2]  = s[q.offset[1]];
                     p[1]  = s[q.offset[0]];
                     p[0]  = s[0];
        }
        return q.dataLen+1;
    }

} // namespace vint
} // namespace warp

#endif // WARP_VINT_H
