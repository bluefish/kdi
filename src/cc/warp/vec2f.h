//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-02-23
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

#ifndef WARP_VEC2F_H
#define WARP_VEC2F_H

namespace warp
{
    /// Two-dimensional vector (in the linear algebra sense) of floats.
    class Vec2f;
}

//----------------------------------------------------------------------------
// Vec2f
//----------------------------------------------------------------------------
class warp::Vec2f
{
    float d[2];

public:
    Vec2f() {}
    Vec2f(float x, float y) { d[0] = x; d[1] = y; }

    float & operator[](int idx) { return d[idx]; }
    float const operator[](int idx) const { return d[idx]; }
};

//----------------------------------------------------------------------------
// Vec2f operators
//----------------------------------------------------------------------------
namespace warp
{
    // Obviously, these operators are far from complete... being lazy.

    inline Vec2f operator-(Vec2f const & a, Vec2f const & b)
    {
        return Vec2f(b[0]-a[0], b[1]-a[1]);
    }
}


#endif // WARP_VEC2F_H
