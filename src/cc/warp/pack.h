//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-06-07
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

#ifndef WARP_PACK_H
#define WARP_PACK_H

#include <stdint.h>

/// Pack four bytes into an MSB ordered integer
#define WARP_PACK4(b0, b1, b2, b3) ( ((uint32_t(b3) & 0xff) << 24) |    \
                                     ((uint32_t(b2) & 0xff) << 16) |    \
                                     ((uint32_t(b1) & 0xff) <<  8) |    \
                                     ((uint32_t(b0) & 0xff)      ) )

#endif // WARP_PACK_H
