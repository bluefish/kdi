//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/warp/charmap.h#1 $
//
// Created 2006/08/07
//
// Copyright 2006 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
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
