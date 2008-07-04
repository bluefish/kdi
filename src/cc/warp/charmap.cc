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

#include <warp/charmap.h>

using namespace warp;

namespace
{
    struct LowerCaseMap
    {
        CharMap m;
        LowerCaseMap()
        {
            char d = 'a' - 'A';
            for(char i = 'A'; i <= 'Z'; ++i)
                m[i] += d;
        }
    };

    struct UpperCaseMap
    {
        CharMap m;
        UpperCaseMap()
        {
            char d = 'A' - 'Z';
            for(char i = 'a'; i <= 'z'; ++i)
                m[i] += d;
        }
    };
}

CharMap::CharMap()
{
    for(int i = 0; i < 256; ++i)
        tr[i] = (char)i;
}

CharMap const & CharMap::toLower()
{
    static LowerCaseMap lcMap;
    return lcMap.m;
}

CharMap const & CharMap::toUpper()
{
    static UpperCaseMap ucMap;
    return ucMap.m;
}
