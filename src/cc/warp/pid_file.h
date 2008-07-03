//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-08-23
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

#ifndef WARP_PID_FILE_H
#define WARP_PID_FILE_H

#include <string>
#include <boost/noncopyable.hpp>

namespace warp
{
    /// Write a file containing the process ID (PID) to a specified
    /// location.  The file is removed when the object is destroyed.
    class PidFile;
}

//----------------------------------------------------------------------------
// PidFile
//----------------------------------------------------------------------------
class warp::PidFile : private boost::noncopyable
{
    std::string fn;

public:
    explicit PidFile(std::string const & fn);
    ~PidFile();

    std::string const & getName() const { return fn; }
};

#endif // WARP_PID_FILE_H
