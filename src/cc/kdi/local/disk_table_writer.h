//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-10-04
// 
// This file is part of KDI.
// 
// KDI is free software; you can redistribute it and/or modify it under the
// terms of the GNU General Public License as published by the Free Software
// Foundation; either version 2 of the License, or any later version.
// 
// KDI is distributed in the hope that it will be useful, but WITHOUT ANY
// WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
// FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
// details.
// 
// You should have received a copy of the GNU General Public License along
// with this program; if not, write to the Free Software Foundation, Inc.,
// 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.
//----------------------------------------------------------------------------

#ifndef KDI_LOCAL_DISK_TABLE_WRITER_H
#define KDI_LOCAL_DISK_TABLE_WRITER_H

#include <kdi/cell.h>
#include <boost/scoped_ptr.hpp>
#include <boost/noncopyable.hpp>

namespace kdi {
namespace local {
    
    /// Serialize an ordered sequence of cells to a file with an index.
    class DiskTableWriter;
    class DiskTableWriterV0;
    class DiskTableWriterV1;
    
    typedef DiskTableWriterV1 CurDiskTableWriter;

} // namespace local
} // namespace kdi

//----------------------------------------------------------------------------
// DiskTableWriter
//----------------------------------------------------------------------------
class kdi::local::DiskTableWriter
    : public CellStream
{
protected:
    class Impl;
    boost::scoped_ptr<Impl> impl;
    bool closed;

public:
    explicit DiskTableWriter(Impl * impl) :
        impl(impl), closed(true) { }

    virtual ~DiskTableWriter();

    void open(std::string const & fn);
    void close();

    void put(Cell const & x);

    // Estimate of size of output were close() called without adding
    // anything else.
    size_t size() const;
};

class kdi::local::DiskTableWriter::Impl
{
public:
    virtual void open(std::string const & fn) = 0;
    virtual void close() = 0;
    virtual void put(Cell const & x) = 0;
    virtual size_t size() const = 0;
    virtual ~Impl() {}
};

class kdi::local::DiskTableWriterV0
    : public kdi::local::DiskTableWriter
{
    class ImplV0;

public:
    explicit DiskTableWriterV0(size_t blockSize);
};

class kdi::local::DiskTableWriterV1
    : public kdi::local::DiskTableWriter
{
    class ImplV1;

public:
    explicit DiskTableWriterV1(size_t blockSize);
};

#endif // KDI_LOCAL_DISK_TABLE_WRITER_H
