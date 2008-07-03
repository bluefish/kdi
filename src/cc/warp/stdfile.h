//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2006 Josh Taylor (Kosmix Corporation)
// Created 2006-01-11
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

#ifndef WARP_STDFILE_H
#define WARP_STDFILE_H

#include "file.h"
#include <stdio.h>
#include <boost/utility.hpp>

namespace warp
{
    class StdFile;
}


//----------------------------------------------------------------------------
// StdFile
//----------------------------------------------------------------------------
/// A File wrapper over the C stdio library.
class warp::StdFile : public File, public boost::noncopyable
{
    FILE * fp;
    std::string fn;

public:
    StdFile(std::string const & uri, char const * mode);
    StdFile(int fd, std::string const & name, char const * mode);
    ~StdFile();

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


#endif // WARP_STDFILE_H
