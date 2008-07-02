//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/warp/vintwriter.cc#1 $
//
// Created 2006/08/24
//
// Copyright 2006 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#include "vintwriter.h"
#include "ex/exception.h"

using namespace warp;
using namespace ex;

//----------------------------------------------------------------------------
// VIntWriter
//----------------------------------------------------------------------------
void VIntWriter::spill()
{
    buf.flip();
    buf.write(fp);
    if(buf.remaining())
        raise<IOError>("failed to spill buffer to file: %s",
                       fp->getName());
    buf.clear();
}

VIntWriter::VIntWriter(FilePtr const & fp) :
    fp(fp), buf(BUF_SIZE)
{
    if(!fp)
        raise<ValueError>("null file");
}

VIntWriter::~VIntWriter()
{
    spill();
}

void VIntWriter::flush()
{
    spill();
}
