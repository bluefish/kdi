//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/warp/vintwriter.h#1 $
//
// Created 2006/08/24
//
// Copyright 2006 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
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
