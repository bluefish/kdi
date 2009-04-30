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

#ifndef WARP_KFSFS_H
#define WARP_KFSFS_H

#include <kfs/KfsClient.h>
#include <warp/fs.h>
#include <string>

namespace warp {
namespace kfs {

    class KfsFilesystem;

} // namespace kfs
} // namespace warp

//----------------------------------------------------------------------------
// KfsFilesystem
//----------------------------------------------------------------------------
class warp::kfs::KfsFilesystem
    : public warp::Filesystem
{
public:
    KfsFilesystem(KFS::KfsClientPtr const & client);

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
    virtual double modificationTime(std::string const & uri);
    virtual double accessTime(std::string const & uri);
    virtual double creationTime(std::string const & uri);

    // Get the initialized client for a URI
    static KFS::KfsClientPtr getClient(std::string const & uri);

private:
    KFS::KfsClientPtr client;
};

#endif // WARP_KFSFS_H
