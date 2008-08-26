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

#include <warp/base64.h>
#include <string.h>

using namespace warp;
using namespace ex;

namespace
{
    char const * const PEM_ALPHABET = 
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz"
        "0123456789+/";

    inline void checkAlphabet(strref_t alphabet)
    {
        // Check size
        if(alphabet.size() != 64)
            raise<ValueError>("alphabet must be 64 unique characters");

        // Check that characters in alphabet are unique
        char uniq[256];
        memset(uniq, 0, sizeof(uniq));
        for(char const * p = alphabet.begin(); p != alphabet.end(); ++p)
        {
            if(uniq[uint8_t(*p)]++)
                raise<ValueError>("alphabet must be 64 unique characters");
        }
    }
}

//----------------------------------------------------------------------------
// Base64Encoder
//----------------------------------------------------------------------------
Base64Encoder::Base64Encoder()
{
    memcpy(alphabet, PEM_ALPHABET, sizeof(alphabet));
}

Base64Encoder::Base64Encoder(strref_t alphabet)
{
    checkAlphabet(alphabet);
    memcpy(this->alphabet, alphabet.begin(), sizeof(this->alphabet));
}

//----------------------------------------------------------------------------
// Base64Decoder
//----------------------------------------------------------------------------
Base64Decoder::Base64Decoder()
{
    memset(number, 0xff, sizeof(number));
    for(uint8_t i = 0; i < 64; ++i)
        number[uint8_t(PEM_ALPHABET[i])] = i;
}

Base64Decoder::Base64Decoder(strref_t alphabet)
{
    checkAlphabet(alphabet);

    memset(number, 0xff, sizeof(number));
    char const * p = alphabet.begin();
    for(uint8_t i = 0; i < 64; ++i)
        number[uint8_t(p[i])] = i;
}
