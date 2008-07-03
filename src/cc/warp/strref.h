//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-02-07
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

#ifndef WARP_STRREF_H
#define WARP_STRREF_H

#include "strutil.h"
#include "string_data.h"
#include "buffer.h"
#include <string>
#include <vector>

namespace warp
{
    /// Class to unify the various ways of representing strings into a
    /// common format.
    class string_wrapper : public str_data_t
    {
        typedef str_data_t super;

    public:
        string_wrapper(super const & str) :
            super(str) {}

        string_wrapper(char const * str) :
            super(str, str + strlen(str)) {}

        string_wrapper(std::string const & str) :
            super(str.c_str(), str.c_str() + str.size()) {}

        string_wrapper(std::vector<char> const & str) :
            super(str.empty() ? 0 : &str[0],
                  str.empty() ? 0 : &str[0] + str.size()) {}

        string_wrapper(std::vector<unsigned char> const & str) :
            super(str.empty() ? 0 : (char const *)&str[0],
                  str.empty() ? 0 : (char const *)&str[0] + str.size()) {}

        string_wrapper(StringOffset const & str) :
            super(str ? str->begin() : 0, str ? str->end() : 0) {}

        string_wrapper(StringData const & str) :
            super(str.begin(), str.end()) {}

        string_wrapper(void const * ptr, size_t sz) :
            super((char const *)ptr, (char const *)ptr + sz) {}

        string_wrapper(const Buffer &buf) :
            super(buf.begin(), buf.end()) {}
    };

    /// Use this type to pass strings into functions, and they'll be
    /// able to receive strings from a variety of source
    /// representations with no end-user type munging.
    typedef string_wrapper const & strref_t;

    /// Use this to wrap arbitrary binary data
    inline string_wrapper binary_data(void const * ptr, size_t len)
    {
        return string_wrapper(ptr, len);
    }

    /// Use this to wrap arbitrary binary data
    template <class T>
    inline string_wrapper binary_data(T const & t)
    {
        return string_wrapper(&t, sizeof(T));
    }

    /// Ordering functor for strref types
    struct strref_lt
    {
        inline bool operator()(strref_t a, strref_t b) const
        {
            return string_compare(a, b);
        }
    };
}

#endif // WARP_STRREF_H
