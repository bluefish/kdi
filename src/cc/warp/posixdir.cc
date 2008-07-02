//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/warp/posixdir.cc#1 $
//
// Created 2006/09/20
//
// Copyright 2006 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#include "posixdir.h"
#include "fs.h"
#include "ex/exception.h"

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

#include "init.h"
WARP_DEFINE_INIT(warp_posixdir)
{
    Directory::registerScheme("file",  &openPosixDir);
    Directory::registerScheme("async", &openPosixDir);
    Directory::registerScheme("mmap", &openPosixDir);
    Directory::setDefaultScheme("file");
}
