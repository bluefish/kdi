//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/warp/vint.h#1 $
//
// Created 2006/08/23
//
// Copyright 2006 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef WARP_VINT_H
#define WARP_VINT_H

#include "buffer.h"

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
