//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2006 Josh Taylor (Kosmix Corporation)
// Created 2006-08-04
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

#ifndef WARP_HSIEH_HASH_H
#define WARP_HSIEH_HASH_H

#include <stdint.h>
#include <stddef.h>

namespace warp
{
    uint32_t hsieh_hash(void const * data, size_t len, uint32_t hash);

    inline uint32_t hsieh_hash(void const * data, size_t len)
    {
        return hsieh_hash(data, len, uint32_t(len));
    }

    inline uint32_t hsieh_hash(char const * begin, char const * end)
    {
        return hsieh_hash(begin, end - begin);
    }
}

#endif // WARP_HSIEH_HASH_H
