//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2006 Josh Taylor (Kosmix Corporation)
// Created 2006-08-18
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

#ifndef WARP_STRHASH_H
#define WARP_STRHASH_H

#include <warp/hash.h>
#include <warp/hsieh_hash.h>
#include <warp/strutil.h>
#include <string>

namespace warp
{
    //------------------------------------------------------------------------
    // simpleHash
    //------------------------------------------------------------------------
    template <class result_t>
    inline result_t simpleHash(char const * begin, char const * end)
    {
        result_t x = result_t(0x79f862aac9cc5a8e);
        for(; begin != end; ++begin)
            x ^= (x << 7) + (x >> 3) + result_t(*begin);
        return x;
    }


    //------------------------------------------------------------------------
    // SimpleHash
    //------------------------------------------------------------------------
    template <class result_t>
    struct SimpleHash
    {
        result_t operator()(char const * begin, char const * end) const {
            return simpleHash<result_t>(begin, end);
        }

        result_t operator()(char const * cStr) const {
            char const * end = cStr;
            while(*end) ++end;
            return operator()(cStr, end);
        }

        result_t operator()(str_data_t const & s) const {
            return operator()(s.begin(), s.end());
        }

        result_t operator()(std::string const & s) const {
            return operator()(s.data(), s.data()+s.length());
        }
    };

    template <class T> struct result_of_hash< SimpleHash<T> >
    {
        typedef T type;
    };

    typedef SimpleHash<uint32_t> SimpleHash32;
    typedef SimpleHash<uint64_t> SimpleHash64;


    //------------------------------------------------------------------------
    // HsiehHash
    //------------------------------------------------------------------------
    struct HsiehHash
    {
        typedef uint32_t result_t;
        result_t operator()(char const * begin, char const * end) const {
            return hsieh_hash(begin, end);
        }

        result_t operator()(char const * cStr) const {
            char const * end = cStr;
            while(*end) ++end;
            return operator()(cStr, end);
        }

        result_t operator()(str_data_t const & s) const {
            return operator()(s.begin(), s.end());
        }

        result_t operator()(std::string const & s) const {
            return operator()(s.data(), s.data()+s.length());
        }
    };

    template <> struct result_of_hash<HsiehHash>
    {
        typedef HsiehHash::result_t type;
    };
}

#endif // WARP_STRHASH_H
