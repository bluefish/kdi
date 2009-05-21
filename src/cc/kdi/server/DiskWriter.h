//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-04-03
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

#ifndef KDI_SERVER_DISKWRITER_H
#define KDI_SERVER_DISKWRITER_H

#include <kdi/server/FragmentWriter.h>
#include <kdi/strref.h>
#include <warp/file.h>
#include <boost/scoped_ptr.hpp>
#include <string>

namespace kdi {
namespace server {

    class DiskWriter;
    class DiskFragmentMaker;

} // namespace server
} // namespace kdi

//----------------------------------------------------------------------------
// DiskWriter
//----------------------------------------------------------------------------
class kdi::server::DiskWriter
    : public kdi::server::FragmentWriter
{
    
public:
    DiskWriter(warp::FilePtr const & out, PendingFileCPtr const & fn,
               size_t blockSize);
    ~DiskWriter();

public:                         // CellOutput API
    void emitCell(strref_t row, strref_t column, int64_t timestamp,
                  strref_t value);
    void emitErasure(strref_t row, strref_t column,
                     int64_t timestamp);

    size_t getCellCount() const;
    size_t getDataSize() const;

public:                         // FragmentWriter API
    PendingFileCPtr finish();

private:
    class Impl;
    boost::scoped_ptr<Impl> impl;
    PendingFileCPtr fn;
};

//----------------------------------------------------------------------------
// DiskFragmentMaker
//----------------------------------------------------------------------------
class kdi::server::DiskFragmentMaker
{
public:
    virtual std::string newDiskFragment() = 0;
};
#endif // KDI_SERVER_DISKWRITER_H
