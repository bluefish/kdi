//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2006 Josh Taylor (Kosmix Corporation)
// Created 2006-12-08
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

#ifndef WARP_PERFFILE_H
#define WARP_PERFFILE_H

#include "file.h"
#include "perfvar.h"
#include <boost/utility.hpp>

namespace warp
{
    class PerformanceFile;
}

//----------------------------------------------------------------------------
// PerformanceFile
//----------------------------------------------------------------------------
/// Performance monitored File interface.
class warp::PerformanceFile : public warp::File, private boost::noncopyable
{
    FilePtr fp;
    PerformanceInteger bytesIn;
    PerformanceInteger bytesOut;

    void setupLogging(PerformanceLog * log);

public:
    /// Construct a PerformanceFile wrapper over the given file
    /// pointer.  Register with the default PerformanceLog.
    explicit PerformanceFile(FilePtr const & fp);

    /// Construct a PerformanceFile wrapper over the given file
    /// pointer.  Register with the given PerformanceLog.
    explicit PerformanceFile(FilePtr const & fp, PerformanceLog * log);

    virtual size_t read(void * dst, size_t elemSz, size_t nElem);
    virtual size_t readline(char * dst, size_t sz, char delim);
    virtual size_t write(void const * src, size_t elemSz, size_t nElem);
    virtual void flush();
    virtual void close();
    virtual off_t tell() const;
    virtual void seek(off_t offset, int whence);
    virtual std::string getName() const;
};

#endif // WARP_PERFFILE_H
