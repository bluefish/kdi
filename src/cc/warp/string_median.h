//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-06-11
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

#ifndef WARP_STRING_MEDIAN_H
#define WARP_STRING_MEDIAN_H

#include <warp/tuple.h>
#include <warp/string_algorithm.h>
#include <warp/string_range.h>
#include <string>
#include <algorithm>
#include <exception>

namespace warp {

    class no_median_error
        : public std::exception
    {
    public:
        virtual char const * what() throw() {
            return "no_median_error";
        }
    };

    /// Choose a minimum-length string strictly between A and B using
    /// lexicographical ordering.  If there are multiple minimum
    /// length candidates, choose the median.  If there are no strings
    /// between A and B, throw no_median_error.
    inline std::string string_median(strref_t a, strref_t b)
    {
        std::string r;

        char const * ap;
        char const * bp;
        if(a.size() < b.size())
            tie(ap,bp) = std::mismatch(a.begin(), a.end(), b.begin());
        else
            tie(bp,ap) = std::mismatch(b.begin(), b.end(), a.begin());

        StringRange const * lo;
        StringRange const * hi;
        if(string_less(bp, b.end(), ap, a.end()))
        {
            lo = &b;
            hi = &a;
            std::swap(ap,bp);
        }
        else
        {
            lo = &a;
            hi = &b;
        }

        for(;; ++ap, ++bp)
        {
            int min;
            int max;

            if(ap != lo->end())
                min = uint8_t(*ap) + 1;
            else
                min = 0;
            
            if(bp != hi->end())
            {
                max = uint8_t(*bp);
                if(bp + 1 == hi->end())
                    --max;
            }
            else
                max = 255;

            if(min <= max)
            {
                r.assign(lo->begin(), ap);
                r += char((min + max) / 2);
                return r;
            }
            else if(ap == lo->end())
                throw no_median_error();
        }
    }

} // namespace warp

#endif // WARP_STRING_MEDIAN_H
