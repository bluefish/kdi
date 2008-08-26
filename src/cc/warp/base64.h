//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
// Created 2008-08-26
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

#ifndef WARP_BASE64_H
#define WARP_BASE64_H

#include <warp/string_range.h>
#include <ex/exception.h>
#include <stdint.h>

namespace warp {

    class Base64Encoder;
    class Base64Decoder;

} // namespace warp

//----------------------------------------------------------------------------
// Base64Encoder
//----------------------------------------------------------------------------
class warp::Base64Encoder
{
    char alphabet[64];

public:
    /// Create an encoder using the standard PEM/MIME alphabet.
    Base64Encoder();

    /// Create an encoder using the given alphabet.  The alphabet must
    /// contain 64 unique characters.
    explicit Base64Encoder(strref_t alphabet);

    /// Encode input to an output iterator
    template <class It>
    It encode_iter(strref_t src, It dst) const
    {
        size_t sz = (src.size() * 4 + 2) / 3;
        char const * s = src.begin();

        // Loop, converting 3 bytes of input into 4 bytes of
        // output
        for(size_t i = sz % 4; i < sz; i += 4)
        {
            uint8_t s0 = *s++;
            uint8_t s1 = *s++;
            uint8_t s2 = *s++;

            *dst = alphabet[                      s0 >> 2 ];    ++dst;
            *dst = alphabet[((s0 << 4) & 0x3f) | (s1 >> 4)];    ++dst;
            *dst = alphabet[((s1 << 2) & 0x3f) | (s2 >> 6)];    ++dst;
            *dst = alphabet[  s2       & 0x3f             ];    ++dst;
        }
            
        // Convert the remaining input, if any.  1 byte of input
        // will be converted to 2 bytes of output, and 2 bytes of
        // input will be converted to 3 bytes of output.
        switch(sz % 4)
        {
            case 2:
            {
                // 1 more byte of input
                uint8_t s0 = *s++;

                *dst = alphabet[                      s0 >> 2 ];    ++dst;
                *dst = alphabet[ (s0 << 4) & 0x3f             ];    ++dst;
                break;
            }

            case 3:
            {
                // 2 more bytes of input
                uint8_t s0 = *s++;
                uint8_t s1 = *s++;

                *dst = alphabet[                      s0 >> 2 ];    ++dst;
                *dst = alphabet[((s0 << 4) & 0x3f) | (s1 >> 4)];    ++dst;
                *dst = alphabet[ (s1 << 2) & 0x3f             ];    ++dst;
                break;
            }
        }

        return dst;
    }

    /// Encode input to a resizable container of characters
    template <class C>
    void encode(strref_t src, C & dst) const
    {
        size_t sz = (src.size() * 4 + 2) / 3;
        dst.resize(sz);
        encode_iter(src, &dst[0]);
    }
};

//----------------------------------------------------------------------------
// Base64Decoder
//----------------------------------------------------------------------------
class warp::Base64Decoder
{
    uint8_t number[256];

public:
    /// Create a decoder using the standard PEM/MIME alphabet.
    Base64Decoder();

    /// Create an decider using the given alphabet.  The alphabet must
    /// contain 64 unique characters.
    explicit Base64Decoder(strref_t alphabet);

    /// Decode input into an output iterator
    template <class It>
    It decode_iter(strref_t src, It dst) const
    {
        using namespace ex;

        if((src.size() % 4) == 1)
            raise<ValueError>("input has invalid length");

        size_t sz = src.size() * 3 / 4;
        char const * s = src.begin();
            
        // Loop, converting 4 bytes of input into 3 bytes of
        // output
        for(size_t i = sz % 3; i < sz; i += 3)
        {
            uint8_t s0 = number[uint8_t(*s++)];
            uint8_t s1 = number[uint8_t(*s++)];
            uint8_t s2 = number[uint8_t(*s++)];
            uint8_t s3 = number[uint8_t(*s++)];

            if((s0 | s1 | s2 | s3) & 0xc0)
                raise<ValueError>("invalid input character");

            *dst = char((s0 << 2) | (s1 >> 4));   ++dst;
            *dst = char((s1 << 4) | (s2 >> 2));   ++dst;
            *dst = char((s2 << 6) | s3);          ++dst;
        }

        // Convert the remaining input, if any.
        switch(sz % 3)
        {
            case 1:
            {
                // 2 more bytes of input
                uint8_t s0 = number[uint8_t(*s++)];
                uint8_t s1 = number[uint8_t(*s++)];
                    
                if((s0 | s1) & 0xc0)
                    raise<ValueError>("invalid input character");

                *dst = char((s0 << 2) | (s1 >> 4));   ++dst;
                break;
            }
                    
            case 2:
            {
                // 3 more bytes of input
                uint8_t s0 = number[uint8_t(*s++)];
                uint8_t s1 = number[uint8_t(*s++)];
                uint8_t s2 = number[uint8_t(*s++)];

                if((s0 | s1 | s2) & 0xc0)
                    raise<ValueError>("invalid input character");

                *dst = char((s0 << 2) | (s1 >> 4));   ++dst;
                *dst = char((s1 << 4) | (s2 >> 2));   ++dst;
                break;
            }
        }

        return dst;
    }

    /// Decode input to a resizable container of characters
    template <class C>
    void decode(strref_t src, C & dst) const
    {
        size_t sz = src.size() * 3 / 4;
        dst.resize(sz);
        decode_iter(src, &dst[0]);
    }
};


#endif // WARP_BASE64_H
