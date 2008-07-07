//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2006 Josh Taylor (Kosmix Corporation)
// Created 2006-06-18
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
