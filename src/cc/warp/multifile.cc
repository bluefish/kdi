//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/warp/multifile.cc#1 $
//
// Created 2006/01/20
//
// Copyright 2006 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
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
