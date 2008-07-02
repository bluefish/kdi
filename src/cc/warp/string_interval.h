//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/warp/string_interval.h#1 $
//
// Created 2007/12/17
//
// Copyright 2007 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef WARP_STRING_INTERVAL_H
#define WARP_STRING_INTERVAL_H

#include <warp/interval.h>
#include <warp/strref.h>
#include <string>

namespace warp {

    /// Return an interval that contains all the strings that start
    /// with the given prefix.
    Interval<std::string> makePrefixInterval(strref_t prefix);
    
} // namespace warp

#endif // WARP_STRING_INTERVAL_H
