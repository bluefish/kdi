//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-12-17
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

#ifndef WARP_STRING_INTERVAL_H
#define WARP_STRING_INTERVAL_H

#include <warp/interval.h>
#include <warp/string_range.h>
#include <string>

namespace warp {

    /// Return an interval that contains all the strings that start
    /// with the given prefix.
    Interval<std::string> makePrefixInterval(strref_t prefix);
    
} // namespace warp

#endif // WARP_STRING_INTERVAL_H
