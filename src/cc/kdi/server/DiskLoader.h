//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-05-01
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

#ifndef KDI_SERVER_DISKLOADER_H
#define KDI_SERVER_DISKLOADER_H

#include <kdi/server/FragmentLoader.h>

namespace kdi {
namespace server {

    class DiskLoader;

} // namespace server
} // namespace kdi

//----------------------------------------------------------------------------
// DiskLoader
//----------------------------------------------------------------------------
class kdi::server::DiskLoader
    : public kdi::server::FragmentLoader
{
public:
    explicit DiskLoader(std::string const & dataRoot) :
        dataRoot(dataRoot) {}

    virtual FragmentCPtr load(std::string const & filename);

private:
    std::string dataRoot;
};


#endif // KDI_SERVER_DISKLOADER_H
