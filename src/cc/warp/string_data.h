//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/warp/string_data.h#1 $
//
// Created 2007/06/07
//
// Copyright 2007 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef WARP_STRING_DATA_H
#define WARP_STRING_DATA_H

#include "strutil.h"
#include "offset.h"
#include "pack.h"
#include "builder.h"
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
            return string_compare_3way(
                begin(), end(), o.begin(), o.end());
    }
    bool operator<(StringData const & o) const {
        return this != &o && string_compare(
            begin(), end(), o.begin(), o.end());
    }

    /// Output string data ranges to BuilderBlock streams using
    /// StringData formatting.
    struct OutputWrapper : public str_data_t
    {
        typedef str_data_t super;
        template <class Range>
        OutputWrapper(Range const & r) : super(r) {}
        template <class It>
        OutputWrapper(It first, It last) : super(first,last) {}
    };
    /// Wrap some string data for Builder output as a StringData type.
    template <class Range>
    static OutputWrapper wrap(Range const & r)
    {
        return OutputWrapper(r);
    }
    /// Wrap some string data for Builder output as a StringData type.
    template <class It>
    static OutputWrapper wrap(It first, It last)
    {
        return OutputWrapper(first,last);
    }
    /// Wrap a string for Builder output as a StringData type.  Do not
    /// wrap temporaries or immediate data.
    static OutputWrapper wrapStr(std::string const & s)
    {
        char const * p = s.c_str();
        return OutputWrapper(p, p + s.size());
    }
};


//----------------------------------------------------------------------------
// Foreign comparison operators
//----------------------------------------------------------------------------
namespace warp
{
    inline bool operator<(StringData const & a, str_data_t const & b)
    {
        return string_compare(a.begin(), a.end(), b.begin(), b.end());
    }

    inline bool operator<(str_data_t const & a, StringData const & b)
    {
        return string_compare(a.begin(), a.end(), b.begin(), b.end());
    }

    inline bool operator<(StringData const & a, std::string const & b)
    {
        return string_compare(a.begin(), a.end(),
                              b.c_str(), b.c_str() + b.size());
    }

    inline bool operator<(std::string const & a, StringData const & b)
    {
        return string_compare(a.c_str(), a.c_str() + a.size(),
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
        BuilderBlock & b, StringData::OutputWrapper const & s)
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
