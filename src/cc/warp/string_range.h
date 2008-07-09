//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
// Created 2008-07-08
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

#ifndef WARP_STRING_RANGE_H
#define WARP_STRING_RANGE_H

#include <warp/string_algorithm.h>
#include <string>
#include <vector>
#include <cstring>
#include <iostream>

#include <boost/type_traits/is_convertible.hpp>
#include <boost/utility/enable_if.hpp>

namespace warp {

    /// A range within a string.  This class does not own the data it
    /// references.  Users of this class must ensure that the string
    /// data in the range remains valid.
    class StringRange
    {
        char const * begin_;
        char const * end_;

        struct unspecified_bool_t {};

    public:
        typedef char const * iterator;
        typedef char const * const_iterator;

    public:
        StringRange() :
            begin_(0), end_(0) {}

        StringRange(StringRange const & o) :
            begin_(o.begin_), end_(o.end_) {}

        StringRange(char const * cstr) :
            begin_(cstr), end_(cstr ? cstr + strlen(cstr) : cstr) {}

        StringRange(char const * begin, char const * end) :
            begin_(begin), end_(end) {}

        StringRange(std::string const & str) :
            begin_(str.c_str()), end_(begin_ + str.size()) {}
    
        StringRange(std::vector<char> const & buf) :
            begin_(buf.empty() ? 0 : &buf[0]), end_(begin_ + buf.size()) {}

        template <class Range>
        StringRange(Range const & r,
                    typename boost::enable_if<
                        boost::is_convertible<
                            typename Range::const_iterator,
                            char const *
                        >
                    >::type * dummy=0
            ) :
            begin_(r.begin()), end_(r.end()) {}

        StringRange(void const * ptr, size_t sz) :
            begin_(static_cast<char const *>(ptr)),
            end_(begin_ + sz) {}

        std::string toString() const {
            return std::string(begin_, end_);
        }

        size_t size() const { return end_ - begin_; }
        bool empty() const { return begin_ == end_; }
        
        operator unspecified_bool_t const *() const {
            unspecified_bool_t const * r = 0;
            r += size();
            return r;
        }

        char const * begin() const { return begin_; }
        char const * end() const { return end_; }

        char operator[](int i) const { return begin_[i]; }
        char front() const { return *begin_; }
        char back() const { return end_[-1]; }
    };

    /// Const reference to string data.  Use this type to pass strings
    /// into functions, and they'll be able to receive strings from a
    /// variety of source representations with no end-user type
    /// munging.
    typedef StringRange const & strref_t;

    /// Do lexicographical comparison of strings interpreted as
    /// unsigned bytes.  Returns true iff a < b.
    inline bool string_equals(strref_t a, strref_t b)
    {
        return string_equals(a.begin(), a.end(), b.begin(), b.end());
    }

    /// Do lexicographical comparison of strings interpreted as
    /// unsigned bytes.  Returns true iff a < b.
    inline bool string_less(strref_t a, strref_t b)
    {
        return string_less(a.begin(), a.end(), b.begin(), b.end());
    }

    /// Do three-way lexicographical comparison of strings interpreted
    /// as unsigned bytes.  Returns a negative number if a < b, zero
    /// if a == b, and a positive number if a > b.
    inline int string_compare(strref_t a, strref_t b)
    {
        return string_compare(a.begin(), a.end(), b.begin(), b.end());
    }

    // Ordering operators
    inline bool operator< (strref_t a, strref_t b)
    {
        return string_less(a,b);
    }
    inline bool operator<=(strref_t a, strref_t b)
    {
        return !(b < a);
    }
    inline bool operator> (strref_t a, strref_t b)
    {
        return b < a;
    }
    inline bool operator>=(strref_t a, strref_t b)
    {
        return !(a < b);
    }
    inline bool operator==(strref_t a, strref_t b)
    {
        return string_equals(a,b);
    }
    inline bool operator!=(strref_t a, strref_t b)
    {
        return !(a == b);
    }

    /// Stream serialization for StringRange
    inline std::ostream & operator<<(std::ostream & o, strref_t s)
    {
        o.write(s.begin(), s.size());
        return o;
    }

    /// Wrap a std::string object.  NB: you're asking for a segfault
    /// if you try to wrap a temporary/immediate string, so don't do
    /// that.
    inline StringRange wrap(std::string const & s)
    {
        return StringRange(s);
    }

    /// Construct a std::string object from a StringRange.
    inline std::string str(strref_t s)
    {
        return s.toString();
    }

    /// Append a StringRange range to a string.
    inline std::string & operator+=(std::string & a, strref_t b)
    {
        a.append(b.begin(), b.end());
        return a;
    }

    /// Use this to wrap arbitrary binary data
    inline StringRange binary_data(void const * ptr, size_t len)
    {
        return StringRange(ptr, len);
    }

    /// Use this to wrap arbitrary binary data
    template <class T>
    inline StringRange binary_data(T const & t)
    {
        return StringRange(&t, sizeof(T));
    }

    /// Ordering functor for strref types
    struct strref_lt
    {
        inline bool operator()(strref_t a, strref_t b) const
        {
            return a < b;
        }
    };

} // namespace warp
    

#endif // WARP_STRING_RANGE_H
