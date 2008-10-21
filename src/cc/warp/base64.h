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
#include <algorithm>

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

    /// Decode input iterator sequence into an output iterator.
    /// Non-alphabet characters in the input are ignored.  If the
    /// input sequence is 1 (mod 4) alphabet characters long, the last
    /// character will be ignored.
    template <class InputIt, class OutputIt>
    OutputIt decode_iter(InputIt src_begin, InputIt src_end,
                         OutputIt dst) const
    {
        using std::find_if;
        InAlphabet inAlphabet(number);

        // Loop, converting 4 bytes of input into 3 bytes of
        // output
        src_begin = find_if(src_begin, src_end, inAlphabet);
        while(src_begin != src_end)
        {
            // Get byte 1 of 4
            uint8_t s0 = number[uint8_t(*src_begin)];
            src_begin = find_if(++src_begin, src_end, inAlphabet);
            if(src_begin == src_end)
            {
                // If this is the only byte left, then we have invalid
                // input.  Ignore the last byte.
                break;
            }

            // Get byte 2 of 4
            uint8_t s1 = number[uint8_t(*src_begin)];
            src_begin = find_if(++src_begin, src_end, inAlphabet);
            if(src_begin == src_end)
            {
                // Only 2 bytes left -- convert to 1 byte of output
                *dst = char((s0 << 2) | (s1 >> 4));   ++dst;
                break;
            }

            // Get byte 3 of 4
            uint8_t s2 = number[uint8_t(*src_begin)];
            src_begin = find_if(++src_begin, src_end, inAlphabet);
            if(src_begin == src_end)
            {
                // Only 3 bytes left -- convert to 2 bytes of output
                *dst = char((s0 << 2) | (s1 >> 4));   ++dst;
                *dst = char((s1 << 4) | (s2 >> 2));   ++dst;
                break;
            }
            
            // Get byte 4 of 4
            uint8_t s3 = number[uint8_t(*src_begin)];
            src_begin = find_if(++src_begin, src_end, inAlphabet);

            // Convert 4 bytes of input to 3 bytes of output
            *dst = char((s0 << 2) | (s1 >> 4));   ++dst;
            *dst = char((s1 << 4) | (s2 >> 2));   ++dst;
            *dst = char((s2 << 6) | s3);          ++dst;
        }

        return dst;
    }

    /// Decode a string into an output iterator
    template <class OutputIt>
    OutputIt decode_iter(strref_t src, OutputIt dst) const
    {
        return decode_iter(src.begin(), src.end(), dst);
    }

    /// Decode input to a resizable container of characters
    template <class C>
    void decode(strref_t src, C & dst) const
    {
        size_t sz = src.size() * 3 / 4;
        dst.resize(sz);
        dst.erase(decode_iter(src, dst.begin()), dst.end());
    }

private:
    /// Functor returns true if the argument is a character in the
    /// decoder alphabet.
    struct InAlphabet
    {
        uint8_t const * number;
        InAlphabet(uint8_t const * number) : number(number) {}
        inline bool operator()(char c) const
        {
            return (number[uint8_t(c)] & 0xc0) == 0;
        }
    };
};


#endif // WARP_BASE64_H
