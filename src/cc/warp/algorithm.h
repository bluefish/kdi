//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-06-14
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

#ifndef WARP_ALGORITHM_H
#define WARP_ALGORITHM_H

#include <ex/exception.h>
#include <utility>
#include <algorithm>

namespace warp
{
    //------------------------------------------------------------------------
    // get_assoc
    //------------------------------------------------------------------------
    /// Get a value out of a pair associative container.  If the value
    /// does not exist, throw a KeyError.
    template <class T>
    typename T::mapped_type const &
    get_assoc(T const & container, typename T::key_type const & key)
    {
        typename T::const_iterator it = container.find(key);
        if(it != container.end())
            return it->second;
        else
            ex::raise<ex::KeyError>("%s", key);
    }

    /// Get a value out of a pair associative container.  If the value
    /// does not exist, use the provided default.
    template <class T>
    typename T::mapped_type const &
    get_assoc(T const & container, typename T::key_type const & key,
             typename T::mapped_type const & dflt)
    {
        typename T::const_iterator it = container.find(key);
        if(it != container.end())
            return it->second;
        else
            return dflt;
    }

    //------------------------------------------------------------------------
    // find_after
    //------------------------------------------------------------------------
    /// Like std::find, except returns the position one past the first
    /// found item.  If no item is found, the end iterator is returned
    /// as usual.  This makes is difficult to determine whether the
    /// item was found or not, but this algorithm is for use when you
    /// don't care.
    template <class It, class T>
    It find_after(It first, It last, T const & x)
    {
        It it = std::find(first, last, x);
        if(it != last)
            ++it;
        return it;
    }

    //------------------------------------------------------------------------
    // is_increasing
    //------------------------------------------------------------------------
    template <class It, class Lt>
    bool is_increasing(It begin, It end, Lt const & lt)
    {
        if(begin == end)
            return true;

        --end;
        while(begin != end)
        {
            if(!lt(*begin, *begin+1))
                return false;
            ++begin;
        }

        return true;
    }

}

#endif // WARP_ALGORITHM_H
