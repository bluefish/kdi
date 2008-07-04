//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2006 Josh Taylor (Kosmix Corporation)
// Created 2006-09-20
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

#ifndef WARP_POSIXDIR_H
#define WARP_POSIXDIR_H

#include <warp/dir.h>
#include <sys/types.h>
#include <dirent.h>
#include <boost/utility.hpp>

namespace warp
{
    class PosixDirectory;
}

//----------------------------------------------------------------------------
// PosixDirectory
//----------------------------------------------------------------------------
class warp::PosixDirectory : public warp::Directory, public boost::noncopyable
{
private:
    DIR * dir;
    std::string name;

public:
    PosixDirectory(std::string const & uri);
    virtual ~PosixDirectory();

    // Directory interface
    virtual void close();
    virtual bool read(std::string & dst);
    virtual std::string getPath() const;
};

#endif // WARP_POSIXDIR_H
