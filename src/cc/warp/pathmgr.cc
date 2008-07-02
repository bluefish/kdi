//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/warp/pathmgr.cc#1 $
//
// Created 2006/08/13
//
// Copyright 2006 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#include "pathmgr.h"
#include "uri.h"
#include "vstring.h"
#include "ex/exception.h"
#include "posixfs.h"

using namespace warp;
using namespace ex;
using namespace std;


//----------------------------------------------------------------------------
// Util
//----------------------------------------------------------------------------
namespace
{
    inline string getCwd()
    {
        return PosixFilesystem::getCwd();
    }
}

//----------------------------------------------------------------------------
// defaultPathCompletion
//----------------------------------------------------------------------------
string warp::defaultPathCompletion(string const & b, string const & p)
{
    enum {
        ALLOW = ( Uri::ANY_SCHEME            |
                  Uri::PATH_SAME_AUTHORITY   |
                  Uri::QUERY_ANY_AUTHORITY   |
                  Uri::QUERY_ANY_PATH        |
                  Uri::QUERY_APPEND          )
    };

    Uri base(wrap(b));
    Uri path(wrap(p));

    VString result;
    base.resolve(path, result, ALLOW);

    return string(result.begin(), result.end());
}


//----------------------------------------------------------------------------
// PathManager
//----------------------------------------------------------------------------
PathManager::PathManager() :
    basePath("file://" + getCwd() + "/"),
    pathComplete(&defaultPathCompletion)
{
}

void PathManager::setBase(string const & base, PathCompletionFunc * comp)
{
    if(!comp)
        pathComplete = &defaultPathCompletion;
    else
        pathComplete = comp;

    string baseBase("file://" + getCwd() + "/");
    basePath = (*pathComplete)(baseBase, base);
}

std::string PathManager::resolve(std::string const & userPath) const
{
    // Check for standard pipe
    if(userPath == "-")
        return "pipe:-";
    
    // Look for scheme in path
    Uri uri(wrap(userPath));
    if(uri.scheme)
        return userPath;

    // Complete path relative to base
    return (*pathComplete)(basePath, userPath);
}
