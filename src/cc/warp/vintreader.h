//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/warp/vintreader.h#1 $
//
// Created 2006/08/24
//
// Copyright 2006 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef WARP_VINTREADER_H
#define WARP_VINTREADER_H

#include "file.h"
#include "buffer.h"
#include "vint.h"
#include <boost/utility.hpp>

namespace warp
{
    class VIntReader;
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
