//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2006 Josh Taylor (Kosmix Corporation)
// Created 2006-08-24
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

#ifndef WARP_VINTWRITER_H
#define WARP_VINTWRITER_H

#include "file.h"
#include "buffer.h"
#include "vint.h"
#include "ex/exception.h"
#include <boost/utility.hpp>

namespace warp
{
    class VIntWriter;
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
