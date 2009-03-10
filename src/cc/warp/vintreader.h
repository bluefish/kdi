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

#ifndef WARP_VINTREADER_H
#define WARP_VINTREADER_H

#include <warp/file.h>
#include <warp/buffer.h>
#include <boost/utility.hpp>

namespace warp
{
    class VIntReader;

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
}

//----------------------------------------------------------------------------
// VIntReader
//----------------------------------------------------------------------------
class warp::VIntReader : public boost::noncopyable
{
    enum { BUF_SIZE = 32 << 10 };

    FilePtr fp;
    Buffer buf;

public:
    VIntReader(FilePtr const & fp);

    template <class Int>
    bool getVInt(Int & x)
    {
        // Optimistic get
        if(warp::getVInt(buf, x))
            return true;

        // That didn't work; refill buffer
        buf.compact();
        buf.read(fp);
        buf.flip();
        return warp::getVInt(buf, x);
    }

    void seek(off_t pos);

    off_t tell() const
    {
        return fp->tell() - buf.remaining();
    }
};

#endif // WARP_VINTREADER_H
