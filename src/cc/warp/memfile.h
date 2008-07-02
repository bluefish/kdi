//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/warp/memfile.h#1 $
//
// Created 2007/04/13
//
// Copyright 2007 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef WARP_MEMFILE_H
#define WARP_MEMFILE_H

#include "file.h"

namespace warp
{
    class MemFile;

    /// Make a file object out of a string
    FilePtr makeMemFile(char const * data);
}


//----------------------------------------------------------------------------
// MemFile
//----------------------------------------------------------------------------
class warp::MemFile : public warp::File
{
    void * data;
    size_t length;
    off_t pos;
    bool writable;

    void * addr(off_t p) const {
        return reinterpret_cast<char *>(data) + p;
    }

public:
    MemFile(void const * data, size_t len, bool writable=false);
    MemFile(void * data, size_t len, bool writable=false);

    virtual size_t read(void * dst, size_t elemSz, size_t nElem);
    virtual size_t readline(char * dst, size_t sz, char delim);
    virtual size_t write(void const * src, size_t elemSz, size_t nElem);
    virtual void close();
    virtual off_t tell() const;
    virtual void seek(off_t offset, int whence);
    virtual std::string getName() const;

    using File::read;
    using File::readline;
    using File::write;
    using File::seek;
};

#endif // WARP_MEMFILE_H
