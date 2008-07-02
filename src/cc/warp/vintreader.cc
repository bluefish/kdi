//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/warp/vintreader.cc#1 $
//
// Created 2006/08/24
//
// Copyright 2006 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#include "vintreader.h"
#include "ex/exception.h"

using namespace warp;
using namespace ex;

//----------------------------------------------------------------------------
// VIntReader
//----------------------------------------------------------------------------
VIntReader::VIntReader(FilePtr const & fp) :
    fp(fp), buf(BUF_SIZE)
{
    if(!fp)
        raise<ValueError>("null file");

    buf.flip();
}

void VIntReader::seek(off_t pos)
{
    // Try to seek within buffered region
    off_t posMax = fp->tell();
    off_t posMin = posMax - (buf.limit() - buf.begin());
    if(pos >= posMin && pos <= posMax)
    {
        // Within buffered region
        buf.position(buf.begin() + (pos - posMin));
    }
    else
    {
        // Seek is outside of buffered region
        fp->seek(pos);
        buf.clear();
        buf.flip();
    }
}
