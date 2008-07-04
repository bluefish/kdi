//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-04-13
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

#ifndef WARP_MEMFILE_H
#define WARP_MEMFILE_H

#include <warp/file.h>

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
