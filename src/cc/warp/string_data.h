//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-06-07
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

#ifndef WARP_STRING_DATA_H
#define WARP_STRING_DATA_H

#include <warp/strutil.h>
#include <warp/offset.h>
#include <warp/pack.h>
#include <warp/builder.h>
#include <boost/utility.hpp>
#include <stdint.h>
#include <stddef.h>
#include <string>
#include <iostream>

namespace warp
{
    struct StringData;
    typedef Offset<StringData> StringOffset;
}

//----------------------------------------------------------------------------
// StringData
//----------------------------------------------------------------------------
struct warp::StringData : public boost::noncopyable
{
    enum {
        TYPECODE = WARP_PACK4('S','t','r','D'),
        VERSION = 0,
    };

    typedef char value_type;
    typedef value_type * iterator;
    typedef value_type const * const_iterator;
    typedef ptrdiff_t difference_type;
    typedef size_t size_type;

    // Note: this string is NOT null terminated.
    uint32_t len;
    char data[1];

    const_iterator begin() const { return data; }
    const_iterator end() const { return data + len; }
    bool empty() const { return len == 0; }
    size_type size() const { return len; }

    std::string toString() const {
        return std::string(begin(), end());
    }
    int cmp(StringData const & o) const {
        if(this == &o)
            return 0;
        else
            return string_compare(
                begin(), end(), o.begin(), o.end());
    }
    bool operator<(StringData const & o) const {
        return this != &o && string_less(
            begin(), end(), o.begin(), o.end());
    }

    /// Output string data ranges to BuilderBlock streams using
    /// StringData formatting.
    struct Wrapper : public StringRange
    {
        typedef StringRange super;
        explicit Wrapper(strref_t s) : super(s) {}
    };
    /// Wrap some string data for Builder output as a StringData type.
    static Wrapper wrap(strref_t s)
    {
        return Wrapper(s);
    }
};


//----------------------------------------------------------------------------
// Foreign comparison operators
//----------------------------------------------------------------------------
namespace warp
{
    inline bool operator<(StringData const & a, strref_t b)
    {
        return string_less(a.begin(), a.end(), b.begin(), b.end());
    }

    inline bool operator<(strref_t a, StringData const & b)
    {
        return string_less(a.begin(), a.end(), b.begin(), b.end());
    }

    inline bool operator<(StringData const & a, std::string const & b)
    {
        return string_less(a.begin(), a.end(),
                           b.c_str(), b.c_str() + b.size());
    }

    inline bool operator<(std::string const & a, StringData const & b)
    {
        return string_less(a.c_str(), a.c_str() + a.size(),
                           b.begin(), b.end());
    }
}


//----------------------------------------------------------------------------
// Output operators
//----------------------------------------------------------------------------
namespace warp
{
    inline BuilderBlock & operator<<(BuilderBlock & b, StringData const & s)
    {
        b.append(s.len);
        b.append(s.data, s.len);
        return b;
    }

    inline BuilderBlock & operator<<(
        BuilderBlock & b, StringData::Wrapper const & s)
    {
        uint32_t sz = s.size();
        b.append(sz);
        b.append(s.begin(), sz);
        return b;
    }

    inline std::ostream & operator<<(std::ostream & o, StringData const & s)
    {
        o.write(s.begin(), s.size());
        return o;
    }

    inline std::ostream & operator<<(std::ostream & o, StringOffset const & s)
    {
        if(s) return o << *s;
        else  return o << "null";
    }
}

#endif // WARP_STRING_DATA_H
