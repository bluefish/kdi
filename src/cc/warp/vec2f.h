//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/warp/vec2f.h#1 $
//
// Created 2007/02/23
//
// Copyright 2007 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
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
