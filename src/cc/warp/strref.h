//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/warp/strref.h#1 $
//
// Created 2007/02/07
//
// Copyright 2007 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
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
