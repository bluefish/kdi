//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2006 Josh Taylor (Kosmix Corporation)
// Created 2006-12-08
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

#include <warp/perffile.h>
#include <warp/perflog.h>
#include <ex/exception.h>

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
