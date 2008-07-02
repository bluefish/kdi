//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/warp/string_interval.cc#1 $
//
// Created 2007/12/17
//
// Copyright 2007 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#include <warp/string_interval.h>

using namespace warp;
using namespace std;

namespace {

    /// Get the next string in a lexicographical ordering that is no
    /// longer than the given prefix.
    /// @returns true if such a string exists
    bool getPrefixSuccessor(strref_t prefix, string & result)
    {
        // The empty string has no prefix successor
        if(!prefix)
            return false;

        // Get the last character and see if it has a successor
        char const * last = prefix.end() - 1;
        if(*last != '\xff')
        {
            // Character has a successor -- form result
            result.reserve(prefix.size());
            result.assign(prefix.begin(), last);
            result.push_back(*last + 1);
            return true;
        }
        else
        {
            // Character has no successor, so prefix has no
            // equal-length successor.  Return a shorter successor if
            // one exists.
            return getPrefixSuccessor(str_data_t(prefix.begin(), last),
                                      result);
        }
    }

}

Interval<string> warp::makePrefixInterval(strref_t prefix)
{
    Interval<string> r;

    // Lower bound is always the prefix string
    r.setLowerBound(str(prefix), BT_INCLUSIVE);

    // Get the prefix successor, if any, for upper bound
    string successor;
    if(getPrefixSuccessor(prefix, successor))
        r.setUpperBound(successor, BT_EXCLUSIVE);
    else
        r.unsetUpperBound();

    return r;
}
