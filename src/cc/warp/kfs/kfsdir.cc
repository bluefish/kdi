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

#include <warp/kfs/kfsdir.h>
#include <warp/kfs/kfsfs.h>
#include <ex/exception.h>

using namespace warp::kfs;
using namespace warp;
using namespace ex;
using KFS::KfsClientPtr;
using std::string;

KfsDirectory::KfsDirectory(KfsClientPtr const & client, string const & uri) :
    uri(uri), idx(0)
{
    if(!client)
        raise<ValueError>("null KfsClient");

    string path = fs::path(uri);
    int r = client->Readdir(path.c_str(), entries);
    if(r != 0)
        raise<IOError>("could not read KFS dir '%s' (path=%s): %s",
                       uri, path, getStdError(-r));
}

KfsDirectory::~KfsDirectory()
{
}

void KfsDirectory::close()
{
}

bool KfsDirectory::read(string & dst)
{
    while(idx < entries.size())
    {
        string const & ent = entries[idx++];
        if(ent != "." && ent != "..")
        {
            dst = ent;
            return true;
        }
    }
    return false;
}

string KfsDirectory::getPath() const
{
    return uri;
}

namespace
{
    DirPtr openKfsDir(string const &uri)
    {
        KfsClientPtr client = KfsFilesystem::getClient(uri);

        DirPtr d(new KfsDirectory(client, uri));
        return d;
    }
}

#include <warp/init.h>
WARP_DEFINE_INIT(warp_kfs_kfsdir)
{
    Directory::registerScheme("kfs", &openKfsDir);
}
