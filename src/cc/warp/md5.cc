//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2005 Josh Taylor (Kosmix Corporation)
// Created 2005-12-06
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

// Adapted from MD5 code at
//   http://userpages.umbc.edu/~mabzug1/cs/md5/md5.html
//
// Serious clean-up by Josh Taylor 2005-12-06

#include <warp/md5.h>

using namespace warp;


// MD5.CC - source code for the C++/object oriented translation and 
//          modification of MD5.

// Translation and modification (c) 1995 by Mordechai T. Abzug 

// This translation/ modification is provided "as is," without express or 
// implied warranty of any kind.

// The translator/ modifier does not claim (1) that MD5 will do what you think 
// it does; (2) that this translation/ modification is accurate; or (3) that 
// this software is "merchantible."  (Language for this disclaimer partially 
// copied from the disclaimer below).

/* based on:

MD5C.C - RSA Data Security, Inc., MD5 message-digest algorithm
MDDRIVER.C - test driver for MD2, MD4 and MD5


Copyright (C) 1991-2, RSA Data Security, Inc. Created 1991. All
rights reserved.

License to copy and use this software is granted provided that it
is identified as the "RSA Data Security, Inc. MD5 Message-Digest
Algorithm" in all material mentioning or referencing this software
or this function.

License is also granted to make and use derivative works provided
that such works are identified as "derived from the RSA Data
Security, Inc. MD5 Message-Digest Algorithm" in all material
mentioning or referencing the derived work.

RSA Data Security, Inc. makes no representations concerning either
the merchantability of this software or the suitability of this
software for any particular purpose. It is provided "as is"
without express or implied warranty of any kind.

These notices must be retained in any copies of any part of this
documentation and/or software.

*/

#include <string.h>

namespace
{
    // Constants for MD5Transform routine.
    int const S11 = 7;
    int const S12 = 12;
    int const S13 = 17;
    int const S14 = 22;
    int const S21 = 5;
    int const S22 = 9;
    int const S23 = 14;
    int const S24 = 20;
    int const S31 = 4;
    int const S32 = 11;
    int const S33 = 16;
    int const S34 = 23;
    int const S41 = 6;
    int const S42 = 10;
    int const S43 = 15;
    int const S44 = 21;

    // ROTATE_LEFT rotates x left n bits.
    inline uint32_t rotate_left(uint32_t x, uint32_t n)
    {
        return (x << n) | (x >> (32-n));
    }

    // F, G, H and I are basic MD5 functions.
    inline uint32_t F(uint32_t x, uint32_t y, uint32_t z)
    {
        return (x & y) | (~x & z);
    }

    inline uint32_t G(uint32_t x, uint32_t y, uint32_t z)
    {
        return (x & z) | (y & ~z);
    }

    inline uint32_t H(uint32_t x, uint32_t y, uint32_t z)
    {
        return x ^ y ^ z;
    }

    inline uint32_t I(uint32_t x, uint32_t y, uint32_t z)
    {
        return y ^ (x | ~z);
    }

    // FF, GG, HH, and II transformations for rounds 1, 2, 3, and 4.
    // Rotation is separate from addition to prevent recomputation.
    inline void FF(uint32_t & a, uint32_t b, uint32_t c, uint32_t d,
                   uint32_t x, uint32_t s, uint32_t ac)
    {
        a += F(b, c, d) + x + ac;
        a = rotate_left (a, s) +b;
    }

    inline void GG(uint32_t & a, uint32_t b, uint32_t c, uint32_t d,
                   uint32_t x, uint32_t s, uint32_t ac)
    {
        a += G(b, c, d) + x + ac;
        a = rotate_left (a, s) +b;
    }

    inline void HH(uint32_t & a, uint32_t b, uint32_t c, uint32_t d,
                   uint32_t x, uint32_t s, uint32_t ac)
    {
        a += H(b, c, d) + x + ac;
        a = rotate_left (a, s) +b;
    }

    inline void II(uint32_t & a, uint32_t b, uint32_t c, uint32_t d,
                   uint32_t x, uint32_t s, uint32_t ac)
    {
        a += I(b, c, d) + x + ac;
        a = rotate_left (a, s) +b;
    }

    template <int LEN>
    inline void encode(uint8_t * dst, uint32_t const * src)
    {
        for(uint32_t const *p = src, *end = src+(LEN>>2); p != end; ++p)
        {
            *dst++ = uint8_t( *p        & 0xff);
            *dst++ = uint8_t((*p >> 8)  & 0xff);
            *dst++ = uint8_t((*p >> 16) & 0xff);
            *dst++ = uint8_t((*p >> 24) & 0xff);
        }
    }

    template <int LEN>
    inline void decode(uint32_t * dst, uint8_t const * src)
    {
        for(uint32_t *p = dst, *end = dst+(LEN>>2); p != end; ++p, src += 4)
        {
            *p = ((uint32_t(src[0])      ) |
                  (uint32_t(src[1]) <<  8) |
                  (uint32_t(src[2]) << 16) |
                  (uint32_t(src[3]) << 24) );
        }
    }
}


//----------------------------------------------------------------------------
// init
//----------------------------------------------------------------------------
void Md5::init()
{
    finalized = false;

    // Nothing counted, so count=0
    count = 0;

    // Load magic initialization constants.
    state[0] = 0x67452301;
    state[1] = 0xefcdab89;
    state[2] = 0x98badcfe;
    state[3] = 0x10325476;
}


//----------------------------------------------------------------------------
// transform
//----------------------------------------------------------------------------
void Md5::transform(uint8_t const * block)
{
    // Block is always 64 bytes.
    // MD5 basic transformation. Transforms state based on block.

    uint32_t a = state[0];
    uint32_t b = state[1];
    uint32_t c = state[2];
    uint32_t d = state[3];

    uint32_t x[16];
    decode<64>(x, block);

    /* Round 1 */
    FF (a, b, c, d, x[ 0], S11, 0xd76aa478); /* 1 */
    FF (d, a, b, c, x[ 1], S12, 0xe8c7b756); /* 2 */
    FF (c, d, a, b, x[ 2], S13, 0x242070db); /* 3 */
    FF (b, c, d, a, x[ 3], S14, 0xc1bdceee); /* 4 */
    FF (a, b, c, d, x[ 4], S11, 0xf57c0faf); /* 5 */
    FF (d, a, b, c, x[ 5], S12, 0x4787c62a); /* 6 */
    FF (c, d, a, b, x[ 6], S13, 0xa8304613); /* 7 */
    FF (b, c, d, a, x[ 7], S14, 0xfd469501); /* 8 */
    FF (a, b, c, d, x[ 8], S11, 0x698098d8); /* 9 */
    FF (d, a, b, c, x[ 9], S12, 0x8b44f7af); /* 10 */
    FF (c, d, a, b, x[10], S13, 0xffff5bb1); /* 11 */
    FF (b, c, d, a, x[11], S14, 0x895cd7be); /* 12 */
    FF (a, b, c, d, x[12], S11, 0x6b901122); /* 13 */
    FF (d, a, b, c, x[13], S12, 0xfd987193); /* 14 */
    FF (c, d, a, b, x[14], S13, 0xa679438e); /* 15 */
    FF (b, c, d, a, x[15], S14, 0x49b40821); /* 16 */

    /* Round 2 */
    GG (a, b, c, d, x[ 1], S21, 0xf61e2562); /* 17 */
    GG (d, a, b, c, x[ 6], S22, 0xc040b340); /* 18 */
    GG (c, d, a, b, x[11], S23, 0x265e5a51); /* 19 */
    GG (b, c, d, a, x[ 0], S24, 0xe9b6c7aa); /* 20 */
    GG (a, b, c, d, x[ 5], S21, 0xd62f105d); /* 21 */
    GG (d, a, b, c, x[10], S22,  0x2441453); /* 22 */
    GG (c, d, a, b, x[15], S23, 0xd8a1e681); /* 23 */
    GG (b, c, d, a, x[ 4], S24, 0xe7d3fbc8); /* 24 */
    GG (a, b, c, d, x[ 9], S21, 0x21e1cde6); /* 25 */
    GG (d, a, b, c, x[14], S22, 0xc33707d6); /* 26 */
    GG (c, d, a, b, x[ 3], S23, 0xf4d50d87); /* 27 */
    GG (b, c, d, a, x[ 8], S24, 0x455a14ed); /* 28 */
    GG (a, b, c, d, x[13], S21, 0xa9e3e905); /* 29 */
    GG (d, a, b, c, x[ 2], S22, 0xfcefa3f8); /* 30 */
    GG (c, d, a, b, x[ 7], S23, 0x676f02d9); /* 31 */
    GG (b, c, d, a, x[12], S24, 0x8d2a4c8a); /* 32 */

    /* Round 3 */
    HH (a, b, c, d, x[ 5], S31, 0xfffa3942); /* 33 */
    HH (d, a, b, c, x[ 8], S32, 0x8771f681); /* 34 */
    HH (c, d, a, b, x[11], S33, 0x6d9d6122); /* 35 */
    HH (b, c, d, a, x[14], S34, 0xfde5380c); /* 36 */
    HH (a, b, c, d, x[ 1], S31, 0xa4beea44); /* 37 */
    HH (d, a, b, c, x[ 4], S32, 0x4bdecfa9); /* 38 */
    HH (c, d, a, b, x[ 7], S33, 0xf6bb4b60); /* 39 */
    HH (b, c, d, a, x[10], S34, 0xbebfbc70); /* 40 */
    HH (a, b, c, d, x[13], S31, 0x289b7ec6); /* 41 */
    HH (d, a, b, c, x[ 0], S32, 0xeaa127fa); /* 42 */
    HH (c, d, a, b, x[ 3], S33, 0xd4ef3085); /* 43 */
    HH (b, c, d, a, x[ 6], S34,  0x4881d05); /* 44 */
    HH (a, b, c, d, x[ 9], S31, 0xd9d4d039); /* 45 */
    HH (d, a, b, c, x[12], S32, 0xe6db99e5); /* 46 */
    HH (c, d, a, b, x[15], S33, 0x1fa27cf8); /* 47 */
    HH (b, c, d, a, x[ 2], S34, 0xc4ac5665); /* 48 */

    /* Round 4 */
    II (a, b, c, d, x[ 0], S41, 0xf4292244); /* 49 */
    II (d, a, b, c, x[ 7], S42, 0x432aff97); /* 50 */
    II (c, d, a, b, x[14], S43, 0xab9423a7); /* 51 */
    II (b, c, d, a, x[ 5], S44, 0xfc93a039); /* 52 */
    II (a, b, c, d, x[12], S41, 0x655b59c3); /* 53 */
    II (d, a, b, c, x[ 3], S42, 0x8f0ccc92); /* 54 */
    II (c, d, a, b, x[10], S43, 0xffeff47d); /* 55 */
    II (b, c, d, a, x[ 1], S44, 0x85845dd1); /* 56 */
    II (a, b, c, d, x[ 8], S41, 0x6fa87e4f); /* 57 */
    II (d, a, b, c, x[15], S42, 0xfe2ce6e0); /* 58 */
    II (c, d, a, b, x[ 6], S43, 0xa3014314); /* 59 */
    II (b, c, d, a, x[13], S44, 0x4e0811a1); /* 60 */
    II (a, b, c, d, x[ 4], S41, 0xf7537e82); /* 61 */
    II (d, a, b, c, x[11], S42, 0xbd3af235); /* 62 */
    II (c, d, a, b, x[ 2], S43, 0x2ad7d2bb); /* 63 */
    II (b, c, d, a, x[ 9], S44, 0xeb86d391); /* 64 */

    state[0] += a;
    state[1] += b;
    state[2] += c;
    state[3] += d;
}


//----------------------------------------------------------------------------
// update
//----------------------------------------------------------------------------
void Md5::update(void const * data, size_t len)
{
    // MD5 block update operation. Continues an MD5 message-digest
    // operation, processing another message block, and updating the
    // context.

    if(finalized)
        init();

    // Compute number of bytes mod 64
    size_t buffer_index = (count >> 3) & 0x3F;

    // Update number of bits
    count += len << 3;

    if(buffer_index + len < sizeof(buffer))
    {
        memcpy(buffer + buffer_index, data, len);
    }
    else
    {
        uint8_t const * p = reinterpret_cast<uint8_t const *>(data);
        uint8_t const * end = p + len;

        if(buffer_index > 0)
        {
            size_t buffer_space = sizeof(buffer) - buffer_index;
            memcpy(buffer + buffer_index, p, buffer_space);
            transform(buffer);
            p += buffer_space;
        }

        while(size_t(end - p) >= sizeof(buffer))
        {
            transform(p);
            p += sizeof(buffer);
        }

        memcpy(buffer, p, end - p);
    }
}


//----------------------------------------------------------------------------
// digest
//----------------------------------------------------------------------------
void Md5::digest(void * dst)
{
    // MD5 finalization. Ends an MD5 message-digest operation, writing
    // the the message digest and zeroizing the context.
    if(!finalized)
    {
        uint8_t const PADDING[64] = {
            0x80, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0,    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0,    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0,    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
        };

        // Save number of bits
        uint8_t bits[8];
        uint32_t count_half = count & 0xffffffff;
        encode<4>(&bits[0], &count_half);
        count_half = (count >> 32) & 0xffffffff;
        encode<4>(&bits[4], &count_half);

        // Pad out to 56 mod 64.
        size_t index = (count >> 3) & 0x3f;
        size_t padLen = (index < 56) ? (56 - index) : (120 - index);
        update(PADDING, padLen);

        // Append length (before padding)
        update(bits, 8);

        finalized = true;
    }
    
    // Store state in digest
    encode<16>(reinterpret_cast<uint8_t *>(dst), state);
}

#include <warp/strutil.h>
std::string warp::md5HexDigest(std::string const & str)
{
    char buf[16];
    Md5::digest(buf, str.c_str(), str.size());
    return hexString(buf, sizeof(buf));
}
