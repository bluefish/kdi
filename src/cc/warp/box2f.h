//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/warp/box2f.h#1 $
//
// Created 2007/02/23
//
// Copyright 2007 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef WARP_BOX2F_H
#define WARP_BOX2F_H

#include "vec2f.h"

namespace warp
{
    /// Axis-aligned bounding box with 2D float coordinates.
    class Box2f;
}

//----------------------------------------------------------------------------
// Box2f
//----------------------------------------------------------------------------
class warp::Box2f
{
    Vec2f p[2];

public:
    Box2f() {}
    Box2f(Vec2f const & minPt, Vec2f const & maxPt)
    {
        p[0] = minPt; p[1] = maxPt;
    }

    /// Get minimum corner of box.
    Vec2f & getMin() { return p[0]; }
    Vec2f const & getMin() const { return p[0]; }
    
    /// Get maximum corner of box.
    Vec2f & getMax() { return p[1]; }
    Vec2f const & getMax() const { return p[1]; }

    // Index into box.
    Vec2f & operator[](int idx) { return p[idx]; }
    Vec2f const & operator[](int idx) const { return p[idx]; }

    /// Get dimensions of box.
    Vec2f getSize() const { return p[1] - p[0]; }

    /// Check to see if box is empty (max point <= min point)
    bool empty() const {
        return p[1][0] <= p[0][0] || p[1][1] <= p[0][1];
    }

    /// Get width of box.
    float getWidth() const { return p[1][0] - p[0][0]; }

    /// Get height of box.
    float getHeight() const { return p[1][1] - p[0][1]; }

    /// Check to see if the box contains the given point.
    bool contains(float x, float y) const
    {
        return (x >= p[0][0] && x < p[1][0] &&
                y >= p[0][1] && y < p[1][1]);
    }

    /// Check to see if the box contains the given point.
    bool contains(Vec2f const & p) const { return contains(p[0], p[1]); }

    /// Check to see if this box contains the given other box.  May
    /// return strange results if either box is invalid (max point
    /// less than min point).
    bool contains(Box2f const & b) const
    {
        return (p[0][0] <= b.p[0][0] &&
                p[1][0] >= b.p[1][0] &&
                p[0][1] <= b.p[0][1] &&
                p[1][1] >= b.p[1][1] );
    }

    /// Check to see if two bounding boxes overlap.  May return
    /// strange results if either box is invalid (max point less than
    /// min point).
    bool overlaps(Box2f const & o) const
    {
        return (o.p[1][0] >   p[0][0] &&
                  p[1][0] > o.p[0][0] &&
                o.p[1][1] >   p[0][1] &&
                  p[1][1] > o.p[0][1] );
    }
};


#endif // WARP_BOX2F_H
