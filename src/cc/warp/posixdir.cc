//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2006 Josh Taylor (Kosmix Corporation)
// Created 2006-09-20
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

#include <warp/posixdir.h>
#include <warp/fs.h>
#include <ex/exception.h>

using namespace warp;
using namespace ex;
using std::string;

//----------------------------------------------------------------------------
// PosixDirectory
//----------------------------------------------------------------------------
PosixDirectory::PosixDirectory(string const & uri) :
    dir(0), name(uri)
{
    string path = fs::path(uri);

    dir = ::opendir(path.c_str());
    if(!dir)
        raise<IOError>("could not open directory '%s' (path=%s): %s",
                       uri, path, getStdError());
}

PosixDirectory::~PosixDirectory()
{
    close();
}

void PosixDirectory::close()
{
    if(dir)
    {
        closedir(dir);
        dir = 0;
    }
}

bool PosixDirectory::read(string & dst)
{
    if(!dir)
        raise<IOError>("read on closed directory");

    for(;;)
    {
        struct dirent * ent = readdir(dir);
        if(!ent)
            return false;
        
        char const * n = ent->d_name;
        if(n[0] == '.' && (n[1] == '\0' || (n[1] == '.' && n[2] == '\0')))
            continue;

        dst = n;
        return true;
    }
}

string PosixDirectory::getPath() const
{
    return name;
}


//----------------------------------------------------------------------------
// Registration
//----------------------------------------------------------------------------
namespace
{
    DirPtr openPosixDir(string const & uri)
    {
        DirPtr d(new PosixDirectory(uri));
        return d;
    }
}

#include <warp/init.h>
WARP_DEFINE_INIT(warp_posixdir)
{
    Directory::registerScheme("file",  &openPosixDir);
    Directory::registerScheme("async", &openPosixDir);
    Directory::registerScheme("mmap", &openPosixDir);
    Directory::setDefaultScheme("file");
}
