//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/warp/charmap.cc#1 $
//
// Created 2006/08/07
//
// Copyright 2006 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#include "charmap.h"

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
