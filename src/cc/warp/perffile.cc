//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/warp/perffile.cc#1 $
//
// Created 2006/12/08
//
// Copyright 2006 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#include "perffile.h"
#include "perflog.h"
#include "ex/exception.h"

using namespace warp;
using namespace ex;
using namespace std;

//----------------------------------------------------------------------------
// PerformanceFile
//----------------------------------------------------------------------------
void PerformanceFile::setupLogging(PerformanceLog * log)
{
    if(!log)
        raise<ValueError>("null performance log");

    bytesIn.setLabel(string("file input ") + fp->getName() + " (bytes)");
    bytesIn = 0;

    bytesOut.setLabel(string("file output ") + fp->getName() + " (bytes)");
    bytesOut = 0;

    log->addItem(&bytesIn);
    log->addItem(&bytesOut);
}

PerformanceFile::PerformanceFile(FilePtr const & fp) :
    fp(fp)
{
    if(!fp)
        raise<ValueError>("null file");

    setupLogging(&PerformanceLog::getCommon());
}

PerformanceFile::PerformanceFile(FilePtr const & fp, PerformanceLog * log) :
    fp(fp)
{
    if(!fp)
        raise<ValueError>("null file");

    setupLogging(log);
}

size_t PerformanceFile::read(void * dst, size_t elemSz, size_t nElem)
{
    size_t n = fp->read(dst, elemSz, nElem);
    bytesIn += n * elemSz;
    return n;
}

size_t PerformanceFile::readline(char * dst, size_t sz, char delim)
{
    size_t n = fp->readline(dst, sz, delim);
    bytesIn += n;
    return n;
}

size_t PerformanceFile::write(void const * src, size_t elemSz, size_t nElem)
{
    size_t n = fp->write(src, elemSz, nElem);
    bytesOut += n * elemSz;
    return n;
}

void PerformanceFile::flush()
{
    fp->flush();
}

void PerformanceFile::close()
{
    fp->close();
}

off_t PerformanceFile::tell() const
{
    return fp->tell();
}

void PerformanceFile::seek(off_t offset, int whence)
{
    fp->seek(offset, whence);
}

std::string PerformanceFile::getName() const
{
    return fp->getName();
}
