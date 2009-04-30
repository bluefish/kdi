//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-03-11
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

#ifndef KDI_SERVER_SERIALIZER_H
#define KDI_SERVER_SERIALIZER_H

#include <kdi/server/CellBuffer.h>
#include <kdi/server/DiskFragment.h>
#include <kdi/server/DiskWriter.h>
#include <kdi/server/FragmentEventListener.h>
#include <kdi/server/TableSchema.h>
#include <kdi/strref.h>
#include <boost/function.hpp>
#include <boost/noncopyable.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/thread.hpp>

namespace kdi {
namespace server {

    class Serializer;

} // namespace server
} // namespace kdi

class kdi::server::Serializer :
    public server::CellOutput
{
    DiskWriter output;
    typedef std::vector<warp::StringRange> frag_vec;
    frag_vec rows;

    void addRow(strref_t row);
    
public:
    Serializer();
    ~Serializer() {};

    void operator()(std::vector<FragmentCPtr> frags, std::string const & fn, 
                    TableSchema::Group const & group);

    void emitCell(strref_t row, strref_t column, int64_t timestamp,
                  strref_t value);

    void emitErasure(strref_t row, strref_t column,
                     int64_t timestamp);

    size_t getCellCount() const;
    size_t getDataSize() const;

    /// Find the next emitted row after row, return false if no more
    bool getNextRow(warp::StringRange & row) const;

};
#endif // KDI_SERVER_SERIALIZER_H
