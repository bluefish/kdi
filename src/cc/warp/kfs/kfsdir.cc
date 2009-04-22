//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/main/cosmix/src/cc/warp/kfs/kfsdir.cc#7 $
//
// Created 2006/09/21
//
// Copyright 2006 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
//
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
