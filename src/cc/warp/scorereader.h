//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/warp/scorereader.h#1 $
//
// Created 2006/07/14
//
// Copyright 2006 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef WARP_SCOREREADER_H
#define WARP_SCOREREADER_H

#include "file.h"
#include "util.h"
#include "buffer.h"
#include "ex/exception.h"
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
