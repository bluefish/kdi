//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-04-01
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

#include <warp/StringTrie.h>

uint32_t const warp::StringTrie_details::BUCKET_SIZES[] = { 0, 1, 3, 7, 17, 37, 67, 127, 257 };
uint32_t const warp::StringTrie_details::GROW_SIZES[]   = { 0, 1, 3, 5, 12, 27, 50,  95, 258 };
