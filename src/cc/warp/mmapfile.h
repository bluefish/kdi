//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2006 Josh Taylor (Kosmix Corporation)
// Created 2006-04-05
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

#ifndef WARP_MMAPFILE_H
#define WARP_MMAPFILE_H

#include <warp/file.h>
#include <boost/utility.hpp>
#include <string>

namespace warp
{
    class MMapFile;
}

//----------------------------------------------------------------------------
// MMapFile
//----------------------------------------------------------------------------
/// A File interface wrapping a memory-mapped file.
/// \todo Add write support.
class warp::MMapFile : public File, public boost::noncopyable
{
    int fd;
    void * ptr;
    size_t length;
    off_t pos;
    std::string uri;

    void * addr(off_t p) const
    {
        return reinterpret_cast<void *>
            (reinterpret_cast<uintptr_t>(ptr) + p);
    }

public:
    explicit MMapFile(std::string const & uri, bool readOnly=true);
    ~MMapFile();

    // File interface
    size_t read(void * dst, size_t elemSz, size_t nElem);
    size_t readline(char * dst, size_t sz, char delim);
    size_t write(void const * src, size_t elemSz, size_t nElem);
    void flush();
    void close();
    off_t tell() const;
    void seek(off_t offset, int whence);
    std::string getName() const;
};


#endif // WARP_MMAPFILE_H
