//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-04-13
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

#include <warp/memfile.h>
#include <warp/shared.h>
#include <ex/exception.h>
#include <boost/format.hpp>

extern "C" {
#include <string.h>
}

using namespace warp;
using namespace ex;


//----------------------------------------------------------------------------
// MemFile
//----------------------------------------------------------------------------
MemFile::MemFile(void const * data, size_t len, bool writable) :
    data(const_cast<void *>(data)), length(len), pos(0), writable(false)
{
    if(writable)
        raise<ValueError>("cannot create writable MemFile on const memory");
}

MemFile::MemFile(void * data, size_t len, bool writable) :
    data(data), length(len), pos(0), writable(writable)
{
}

size_t MemFile::read(void * dst, size_t elemSz, size_t nElem)
{
    if(!data)
        raise<IOError>("read on closed file");

    size_t sz = elemSz * nElem;
    if(pos + sz > length)
    {
        nElem = (length - pos) / elemSz;
        sz = elemSz * nElem;
    }

    memcpy(dst, addr(pos), sz);
    pos += sz;
    return nElem;
}

size_t MemFile::readline(char * dst, size_t sz, char delim)
{
    if(!data)
        raise<IOError>("readline on closed file");

    if(!sz)
        return 0;
    --sz;
  
    if(pos + sz > length)
        sz = length - pos;

    char const * p = reinterpret_cast<char const *>(addr(pos));
    size_t len = 0;
    while(len < sz)
    {
        char c = *p++;
        dst[len++] = c;
        if(c == delim)
            break;
    }

    pos += len;
    
    dst[len] = '\0';
    return len;
}

size_t MemFile::write(void const * src, size_t elemSz, size_t nElem)
{
    if(!data)
        raise<IOError>("write on closed file");

    if(!writable)
        raise<IOError>("write not supported on read-only memfile");

    size_t sz = elemSz * nElem;
    if(pos + sz > length)
    {
        nElem = (length - pos) / elemSz;
        sz = elemSz * nElem;
    }

    memcpy(addr(pos), src, sz);
    pos += sz;
    return nElem;
}

void MemFile::close()
{
    data = 0;
}

off_t MemFile::tell() const
{
    if(!data)
        raise<IOError>("tell on closed file");

    return pos;
}

void MemFile::seek(off_t offset, int whence)
{
    if(!data)
        raise<IOError>("seek on closed file");

    off_t p;
    switch(whence)
    {
        case SEEK_SET:
            p = offset;
            break;
            
        case SEEK_CUR:
            p = offset + pos;
            break;

        case SEEK_END:
            p = offset + length;
            break;

        default:
            raise<IOError>("unknown seek mode: %d", whence);
    }

    if(p < 0)
        raise<IOError>("seek to negative position: %d", p);
    else if(p > (off_t)length)
        raise<IOError>("seek past end of buffer: %d", p);
    else
        pos = p;
}

std::string MemFile::getName() const
{
    return (boost::format("memfile:#0x%p") % data).str();
}

FilePtr warp::makeMemFile(char const * data)
{
    return newShared<MemFile>(data, strlen(data));
}
