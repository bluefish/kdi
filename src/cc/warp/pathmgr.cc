//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2006 Josh Taylor (Kosmix Corporation)
// Created 2006-08-13
//
// This file is part of KDI.
// 
// KDI is free software; you can redistribute it and/or modify it
// under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// any later version.
// 
// KDI is distributed in the hope that it will be useful, but WITHOUT
// ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
// or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public
// License for more details.
// 
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
// 02110-1301, USA.
//----------------------------------------------------------------------------

#include <warp/pathmgr.h>
#include <warp/uri.h>
#include <warp/vstring.h>
#include <ex/exception.h>
#include <warp/posixfs.h>

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
