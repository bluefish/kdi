//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
// Created 2008-07-08
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

#ifndef WARP_STRING_ALGORITHM_H
#define WARP_STRING_ALGORITHM_H

#include <cstring>
#include <cstddef>
#include <cassert>

namespace warp {

    /// Do equality comparison of string ranges.  Returns true if
    /// (a0,a1) is equal to (b0,b1).
    inline bool string_equals(char const * a0, char const * a1,
                              char const * b0, char const * b1)
    {
        ptrdiff_t aLen = a1 - a0;
        ptrdiff_t bLen = b1 - b0;
        assert(aLen >= 0 && bLen >= 0);
        return aLen == bLen && (a0 == b0 || 0 == memcmp(a0, b0, aLen));
    }

    /// Do lexicographical comparison of strings interpreted as
    /// unsigned bytes.  Returns true iff (a0,a1) < (b0,b1).
    inline bool string_less(char const * a0, char const * a1,
                            char const * b0, char const * b1)
    {
        ptrdiff_t aLen = a1 - a0;
        ptrdiff_t bLen = b1 - b0;
        assert(aLen >= 0 && bLen >= 0);
        if(aLen < bLen)
            return memcmp(a0, b0, aLen) <= 0;
        else
            return memcmp(a0, b0, bLen) < 0;
    }

    /// Do three-way lexicographical comparison of strings interpreted
    /// as unsigned bytes.  Returns a negative number if (a0,a1) is
    /// less than (b0,b1), zero if they're equal, or a positive number
    /// if (a0,a1) is greater than (b0,b1).
    inline int string_compare(char const * a0, char const * a1,
                              char const * b0, char const * b1)
    {
        ptrdiff_t aLen = a1 - a0;
        ptrdiff_t bLen = b1 - b0;
        assert(aLen >= 0 && bLen >= 0);
        if(int cmp = memcmp(a0, b0, (aLen < bLen ? aLen : bLen)))
            return cmp;
        else
            return aLen - bLen;
    }

} // namespace warp

#endif // WARP_STRING_ALGORITHM_H
