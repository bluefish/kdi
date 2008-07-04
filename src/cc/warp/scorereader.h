//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2006 Josh Taylor (Kosmix Corporation)
// Created 2006-07-14
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

#ifndef WARP_SCOREREADER_H
#define WARP_SCOREREADER_H

#include <warp/file.h>
#include <warp/util.h>
#include <warp/buffer.h>
#include <ex/exception.h>
#include <boost/utility.hpp>

namespace warp
{
    class ScoreReader;
}


//----------------------------------------------------------------------------
// ScoreReader
//----------------------------------------------------------------------------
class warp::ScoreReader : public boost::noncopyable
{
    enum { BUF_SIZE = 32 << 10 };
    typedef int64_t seq_t;

    Buffer buf;
    FilePtr fp;
    seq_t begin;
    seq_t end;

public:
    ScoreReader(FilePtr const & fp) :
        buf(BUF_SIZE), fp(fp), begin(0), end(0)
    {
        if(!fp)
            ex::raise<ex::IOError>("null score file");
    }

    float getScore(int64_t seq)
    {
        if(seq < begin || seq >= end)
        {
            if(seq < 0)
                ex::raise<ex::IOError>("invalid seqnum '%1%'", seq);

            buf.clear();
            fp->seek(seq * 4);
            buf.read(fp);
            buf.flip();

            begin = seq;
            end = begin + buf.remaining() / 4;

            if(begin == end)
                ex::raise<ex::IOError>("couldn't read seqnum '%1%'", seq);
        }

#if 0  // No byteswap

        float f = *(float *)(buf.begin() + (seq - begin) * 4);

#else  // Do byteswap

        float f;
        
        char * d = (char *)&f + 3;
        char const * s = buf.begin() + (seq - begin) * 4;
        
        *d = *s;
        *--d = *++s;
        *--d = *++s;
        *--d = *++s;

#endif

        return f;
    }
};


#endif // WARP_SCOREREADER_H
