//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2006 Josh Taylor (Kosmix Corporation)
// Created 2006-09-19
// 
// This file is part of the warp library.
// 
// The warp library is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by the
// Free Software Foundation; either version 2 of the License, or any later
// version.
// 
// The warp library is distributed in the hope that it will be useful, but
// WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General
// Public License for more details.
// 
// You should have received a copy of the GNU General Public License along
// with this program; if not, write to the Free Software Foundation, Inc.,
// 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.
//----------------------------------------------------------------------------

#ifndef WARP_POSIXFS_H
#define WARP_POSIXFS_H

#include <warp/fs.h>

namespace warp
{
    class PosixFilesystem;
}

//----------------------------------------------------------------------------
// PosixFilesystem
//----------------------------------------------------------------------------
class warp::PosixFilesystem : public warp::Filesystem
{
private:
    // Do not construct this object.  Use get().
    PosixFilesystem();

public:
    // Filesystem interface
    virtual bool mkdir(std::string const & uri);
    virtual bool remove(std::string const & uri);
    virtual void rename(std::string const & sourceUri,
                        std::string const & targetUri,
                        bool overwrite);
    virtual size_t filesize(std::string const & uri);
    virtual bool exists(std::string const & uri);
    virtual bool isDirectory(std::string const & uri);
    virtual bool isFile(std::string const & uri);
    virtual bool isEmpty(std::string const & uri);
    virtual Timestamp modificationTime(std::string const & uri);
    virtual Timestamp accessTime(std::string const & uri);
    virtual Timestamp creationTime(std::string const & uri);

    // Extra functions

    /// Get current working directory.  The working directory is
    /// returned as a simple path -- the scheme, authority, query, and
    /// fragment sections of the URI are undefined.
    static std::string getCwd();

    // Get implementation
    static FsPtr get();
};

#endif // WARP_POSIXFS_H
