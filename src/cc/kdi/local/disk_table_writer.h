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

} // namespace local
} // namespace kdi

//----------------------------------------------------------------------------
// DiskTableWriter
//----------------------------------------------------------------------------
class kdi::local::DiskTableWriter
    : public CellStream
{
    class Impl;
    boost::scoped_ptr<Impl> impl;

public:
    DiskTableWriter(std::string const & fn, size_t blockSize);
    ~DiskTableWriter();

    void close();
    void put(Cell const & x);
};

#endif // KDI_LOCAL_DISK_TABLE_WRITER_H
