//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-06-14
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

#ifndef WARP_VARSUB_H
#define WARP_VARSUB_H

#include <string>
#include <map>

namespace warp
{
    /// Substitute variables of the form "$KEY", "$(KEY)", "${KEY}",
    /// or "$[KEY]" with the value associated with "KEY" in the given
    /// variable map.  If the value is not found, substitute an empty
    /// string.  Literal dollar signs can be including using "$$".
    /// Also, if a single "$" appears with an invalid variable
    /// specification, the literal "$" will be retained.  The
    /// delimited forms of variable naming may contain any characters
    /// except for the end delimiter.  The non-delimited form must be
    /// a valid identifier: an alphabetic character followed by zero
    /// or more alpha-numeric characters.
    std::string varsub(std::string const & pattern,
                       std::map<std::string, std::string> const & vars);
}

#endif // WARP_VARSUB_H
