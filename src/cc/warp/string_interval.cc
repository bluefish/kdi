//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-12-17
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
