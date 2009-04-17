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

#ifndef KDI_SERVER_DISKOUTPUT_H
#define KDI_SERVER_DISKOUTPUT_H

#include <boost/scoped_ptr.hpp>
#include <kdi/strref.h>
#include <kdi/server/CellOutput.h>

namespace kdi {
namespace server {

    class DiskOutput;

} // namespace server
} // namespace kdi

//----------------------------------------------------------------------------
// CellOutput
//----------------------------------------------------------------------------
class kdi::server::DiskOutput : public kdi::server::CellOutput
{
protected: 
    class Impl;
    boost::scoped_ptr<Impl> impl;
    bool closed;
    
public:
    DiskOutput(size_t blockSize);
    ~DiskOutput();

    void open(std::string const & fn);
    void close();

    void emitCell(strref_t row, strref_t column, int64_t timestamp,
                  strref_t value);
    void emitErasure(strref_t row, strref_t column,
                     int64_t timestamp);

    size_t getCellCount() const;
    size_t getDataSize() const;
};

#endif // KDI_SERVER_DISKOUTPUT_H
