//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2006 Josh Taylor (Kosmix Corporation)
// Created 2006-08-13
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

#ifndef WARP_PATHMGR_H
#define WARP_PATHMGR_H

#include <warp/singleton.h>
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
