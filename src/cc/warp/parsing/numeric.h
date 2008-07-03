//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
// Created 2008-03-21
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

#ifndef WARP_PARSING_NUMERIC_H
#define WARP_PARSING_NUMERIC_H

#include <boost/spirit.hpp>
#include <stdint.h>

namespace warp {
namespace parsing {

    using boost::spirit::int_parser;
    using boost::spirit::uint_parser;

    int_parser<int64_t, 10, 1, -1> const int64_p = int_parser<int64_t, 10, 1, -1>();
    uint_parser<int, 10, 2, 2> const uint2_p = uint_parser<int, 10, 2, 2>();
    uint_parser<int, 10, 4, 4> const uint4_p = uint_parser<int, 10, 4, 4>();

} // namespace parsing
} // namespace warp

#endif // WARP_PARSING_NUMERIC_H
