//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/warp/pathmgr.h#1 $
//
// Created 2006/08/13
//
// Copyright 2006 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef WARP_PATHMGR_H
#define WARP_PATHMGR_H

#include "singleton.h"
#include <string>

namespace warp
{
    class PathManager;

    typedef std::string (PathCompletionFunc)(std::string const & base,
                                             std::string const & path);

    extern std::string defaultPathCompletion(std::string const & base,
                                             std::string const & path);
}

//----------------------------------------------------------------------------
// PathManager
//----------------------------------------------------------------------------
class warp::PathManager : public warp::Singleton<PathManager>
{
    friend class Singleton<PathManager>;

    std::string basePath;
    PathCompletionFunc * pathComplete;

    PathManager();

public:
    std::string const & getBase() const { return basePath; }
    void setBase(std::string const & base, PathCompletionFunc * comp=0);
    std::string resolve(std::string const & userPath) const;
};


#endif // WARP_PATHMGR_H
