//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2006 Josh Taylor (Kosmix Corporation)
// Created 2006-09-21
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

#ifndef WARP_KFSDIR_H
#define WARP_KFSDIR_H

#include <kfs/KfsClient.h>
#include <warp/dir.h>
#include <vector>
#include <string>

namespace warp {
namespace kfs {

    class KfsDirectory;

} // namespace kfs
} // namespace warp

//----------------------------------------------------------------------------
// KfsDirectory
//----------------------------------------------------------------------------
class warp::kfs::KfsDirectory
    : public warp::Directory
{
public:
    KfsDirectory(KFS::KfsClientPtr const & client, std::string const & uri);
    virtual ~KfsDirectory();

    // Directory interface
    virtual void close();
    virtual bool read(std::string & dst);
    virtual std::string getPath() const;

private:
    std::string uri;
    std::vector<std::string> entries;
    std::vector<std::string>::size_type idx;
};


#endif // WARP_KFSDIR_H
