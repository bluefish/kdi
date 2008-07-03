//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2006 Josh Taylor (Kosmix Corporation)
// Created 2006-08-07
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

#ifndef WARP_CHARMAP_H
#define WARP_CHARMAP_H

#include <stdint.h>

namespace warp
{
    class CharMap;
}

//----------------------------------------------------------------------------
// CharMap
//----------------------------------------------------------------------------
class warp::CharMap
{
    char tr[256];

public:
    CharMap();
    
    char & operator[](char x) {
        return tr[(uint8_t)x];
    }

    char operator[](char x) const {
        return tr[(uint8_t)x];
    }

    inline char operator()(char x) const {
        return tr[(uint8_t)x];
    }

    static CharMap const & toLower();
    static CharMap const & toUpper();
};


#endif // WARP_CHARMAP_H
