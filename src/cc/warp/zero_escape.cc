//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: warp/zero_escape.cc $
//
// Created 2008/03/17
//
// Copyright 2008 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#include <warp/zero_escape.h>
#include <sstream>

using namespace warp;

std::string warp::zeroEscape(strref_t s)
{
    std::ostringstream oss;
    oss << ZeroEscape(s);
    return oss.str();
}

std::string warp::zeroUnescape(strref_t s)
{
    std::ostringstream oss;
    oss << ZeroUnescape(s);
    return oss.str();
}
