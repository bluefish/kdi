//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2006 Josh Taylor (Kosmix Corporation)
// Created 2006-01-20
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

#include "multifile.h"
#include "ex/exception.h"

using namespace warp;
using namespace ex;

//----------------------------------------------------------------------------
// MultiFile
//----------------------------------------------------------------------------
MultiFile::MultiFile(gen_t const & fileGenerator) :
    fileGenerator(fileGenerator)
{
}

MultiFile::~MultiFile()
{
}

void MultiFile::setGenerator(gen_t const & gen)
{
    fileGenerator = gen;
}

bool MultiFile::openNext()
{
    if(!fileGenerator)
        raise<IOError>("no file generator");

    try {
        currentFile = fileGenerator->next();
        return true;
    }
    catch(StopIteration const & ex) {
        currentFile.reset();
        return false;
    }
}

void MultiFile::close()
{
    currentFile.reset();
}

size_t MultiFile::read(void * dst, size_t elemSz, size_t nElem)
{
    for(;;)
    {
        if(!currentFile && !openNext())
            return 0;

        if(size_t readSz = currentFile->read(dst, elemSz, nElem))
            return readSz;
        else
            currentFile.reset();
    }
}

size_t MultiFile::readline(char * dst, size_t sz, char delim)
{
    for(;;)
    {
        if(!currentFile && !openNext())
            return 0;

        if(size_t len = currentFile->readline(dst, sz, delim))
            return len;
        else
            currentFile.reset();
    }
}

size_t MultiFile::write(void const * src, size_t elemSz, size_t nElem)
{
    if(!currentFile && !openNext())
        raise<IOError>("file generator exhausted");

    return currentFile->write(src, elemSz, nElem);
}

void MultiFile::flush()
{
    if(currentFile)
        currentFile->flush();
}

off_t MultiFile::tell() const
{
    if(currentFile)
        return currentFile->tell();
    else
        return 0;
}

void MultiFile::seek(off_t offset, int whence)
{
    raise<NotImplementedError>("MultiFile does not implement seek()");
}

std::string MultiFile::getName() const
{
    if(currentFile)
        return currentFile->getName();
    else
        return std::string();
}


//----------------------------------------------------------------------------
// FlushMultiFile
//----------------------------------------------------------------------------
FlushMultiFile::FlushMultiFile(gen_t const & gen) :
    super(gen)
{
}

void FlushMultiFile::flush()
{
    super::flush();
    super::close();
}
