//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/warp/tmpfile.h#1 $
//
// Created 2006/04/03
//
// Copyright 2006 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef WARP_TMPFILE_H
#define WARP_TMPFILE_H

#include "file.h"
#include "generator.h"
#include <string>

namespace warp
{
    //------------------------------------------------------------------------
    // TmpFileGen
    //------------------------------------------------------------------------
    class TmpFileGen : public Generator<FilePtr>
    {
        std::string rootDir;
        bool madeDir;

    public:
        TmpFileGen(std::string const & rootDir);
        FilePtr next();
    };

    /// Open a unique temporary file in the given directory.  The
    /// filename will be determined automatically, but the prefix can
    /// be specified.  The directory must reside on a local
    /// filesystem.
    FilePtr openTmpFile(std::string const & dir, std::string const & prefix);
}

#endif // WARP_TMPFILE_H
