//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/warp/fsutil.h#1 $
//
// Created 2006/06/18
//
// Copyright 2006 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef WARP_FSUTIL_H
#define WARP_FSUTIL_H

#include <string>
#include <vector>

namespace warp
{
    void findFilesNumericSorted(std::string const & dir,
                                std::string const & ext,
                                std::vector<std::string> & out);

    void findDirsNumericSorted(std::string const & dir,
                               std::vector<std::string> & out);

    void getExistingDirs(std::string const & base,
                         std::vector<std::string> const & children,
                         std::vector<std::string> & out,
                         bool reportMissing);

    void getExistingFiles(std::string const & base,
                          std::vector<std::string> const & children,
                          std::string const & ext,
                          std::vector<std::string> & out,
                          bool reportMissing);
}

#endif // WARP_FSUTIL_H
