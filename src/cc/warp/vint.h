//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2006 Josh Taylor (Kosmix Corporation)
// Created 2006-08-23
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

#ifndef WARP_VINT_H
#define WARP_VINT_H

#include <warp/buffer.h>

namespace warp
{
    /// Get a variable-length integer from a Buffer.  Return true if
    /// Buffer contained a complete int.
    template <class Int>
    bool getVInt(Buffer & buf, Int & x)
    {
        char * p = buf.position();
        char * pEnd = buf.limit();

        uint64_t i = 0;
        for(int shift = 0; p != pEnd; shift += 7)
        {
            char b = *p++;
            i |= uint64_t(b & 0x7f) << shift;
            if((b & 0x80) == 0)
            {
                buf.position(p);
                x = i;
                return true;
            }
        }
        return false;
    }

    /// Put a variable-length integer into a Buffer.  Return true if
    /// there was enough room.
    template <class Int>
    bool putVInt(Buffer & buf, Int x)
    {
        uint64_t const  LOW_MASK(0x7f);
        char const      HIGH_BIT(0x80);

        char * p = buf.position();
        char * pEnd = buf.limit();
        uint64_t i(x);
        while(p != pEnd)
        {
            if(i & ~LOW_MASK)
            {
                *p++ = char(i & LOW_MASK) | HIGH_BIT;
                i >>= 7;
            }
            else
            {
                *p++ = char(i);
                buf.position(p);
                return true;
            }
        }
        return false;
    }
}

#endif // WARP_VINT_H
