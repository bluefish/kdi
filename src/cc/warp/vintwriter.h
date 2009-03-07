//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2006 Josh Taylor (Kosmix Corporation)
// Created 2006-08-24
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

#ifndef WARP_VINTWRITER_H
#define WARP_VINTWRITER_H

#include <warp/file.h>
#include <warp/buffer.h>
#include <warp/vint.h>
#include <ex/exception.h>
#include <boost/utility.hpp>

namespace warp
{
    class VIntWriter;

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

//----------------------------------------------------------------------------
// VIntWriter
//----------------------------------------------------------------------------
class warp::VIntWriter : public boost::noncopyable
{
    enum { BUF_SIZE = 32 << 10 };

    FilePtr fp;
    Buffer buf;

    void spill();

public:
    VIntWriter(FilePtr const & fp);
    ~VIntWriter();

    void flush();

    template <class Int>
    void putVInt(Int x)
    {
        using namespace ex;

        // Optimistic put
        if(!warp::putVInt(buf, x))
        {
            // No good; spill buffer
            spill();
            if(!warp::putVInt(buf, x))
                raise<IOError>("couldn't write VInt: %1%", x);
        }
    }

    off_t tell() const
    {
        return fp->tell() + buf.consumed();
    }
};


#endif // WARP_VINTWRITER_H
