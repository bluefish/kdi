//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-04-29
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

#ifndef KDI_SERVER_DISKWRITERFACTORY_H
#define KDI_SERVER_DISKWRITERFACTORY_H

#include <kdi/server/FragmentWriterFactory.h>
#include <string>

namespace kdi {
namespace server {

    class DiskWriterFactory;

} // namespace server
} // namespace kdi

//----------------------------------------------------------------------------
// DiskWriterFactory
//----------------------------------------------------------------------------
class kdi::server::DiskWriterFactory
    : public kdi::server::FragmentWriterFactory
{
public:
    DiskWriterFactory(std::string const & dataRoot) :
        dataRoot(dataRoot) {}

    virtual std::auto_ptr<FragmentWriter>
    start(TableSchema const & schema, int groupIndex);

private:
    std::string const dataRoot;
};

#endif // KDI_SERVER_DISKWRITERFACTORY_H
