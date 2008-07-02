//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/warp/tmpfile.cc#1 $
//
// Created 2006/04/03
//
// Copyright 2006 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#include "tmpfile.h"
#include "stdfile.h"
#include "fs.h"
#include "posixfs.h"
#include "ex/exception.h"

#include <algorithm>
#include <stdlib.h>

using namespace warp;
using namespace ex;
using namespace std;


//----------------------------------------------------------------------------
// TmpFileGen
//----------------------------------------------------------------------------
TmpFileGen::TmpFileGen(string const & rootDir) :
    rootDir(rootDir), madeDir(false)
{
    FsPtr p = Filesystem::get(rootDir);
    if(p != PosixFilesystem::get())
        raise<RuntimeError>("TmpFileGen only works on Posix filesystem");

    if(p->exists(rootDir) && !p->isDirectory(rootDir))
        raise<IOError>("tmp file root is not a directory: %s", rootDir);
}

FilePtr TmpFileGen::next()
{
    if(!madeDir)
    {
        fs::makedirs(rootDir);
        madeDir = true;
    }

    return openTmpFile(rootDir, "tmp-");
}

FilePtr warp::openTmpFile(string const & dir, string const & prefix)
{
    string fn = fs::resolve(dir, prefix + "XXXXXX");
    string p = fs::path(fn);
    int fd = mkstemp(&p[0]);
    if(fd < 0)
        raise<IOError>("mkstemp failed for '%s': %s", p, getStdError());

    // mkstemp modifies filename in place.  Update filename URI.
    fn = fs::replacePath(fn, p);

    FilePtr fp(new StdFile(fd, fn, "wb"));
    return fp;
}
