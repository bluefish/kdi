//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
// Created 2008-03-17
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

#ifndef WARP_ZERO_ESCAPE_H
#define WARP_ZERO_ESCAPE_H

#include <warp/string_range.h>
#include <ex/exception.h>
#include <ostream>
#include <string>

namespace warp {

    /// An ostream adapter that will escape nulls in a string so that
    /// two zeros never appear next to each other (the string
    /// "\x00\x00" does not appear in the output string).  When a
    /// sequence of one or more zeros is found, it is replaced by a
    /// zero and the count of zeros in the sequence (up to 255).  So a
    /// single zero is escaped as "\x00\x01".  If more than 255 zeros
    /// appear in a row, they will be encoded in multiple escape
    /// sequences.  This transformation maintains lexicographical
    /// ordering on the string.
    class ZeroEscape;

    /// An ostream adapter that will decode a string encoded with
    /// ZeroEscape.
    class ZeroUnescape;

    /// Escape nulls in a string so that two zeros never appear next
    /// to each other (the string "\x00\x00" does not appear in the
    /// output string).  When a sequence of one or more zeros is
    /// found, it is replaced by a zero and the count of zeros in the
    /// sequence (up to 255).  So a single zero is escaped as
    /// "\x00\x01".  If more than 255 zeros appear in a row, they will
    /// be encoded in multiple escape sequences.  This transformation
    /// maintains lexicographical ordering on the string.
    std::string zeroEscape(strref_t s);

    /// Decode a string encoded with zeroEscape.
    std::string zeroUnescape(strref_t s);

} // namespace warp

//----------------------------------------------------------------------------
// ZeroEscape
//----------------------------------------------------------------------------
class warp::ZeroEscape : public warp::StringRange
{
public:
    explicit ZeroEscape(strref_t str) : StringRange(str) {}
};

namespace warp {

    std::ostream & operator<<(std::ostream & out, ZeroEscape const & x)
    {
        char const * begin = x.begin();
        char const * end = x.end();
        for(;;)
        {
            char const * p = std::find(begin, end, '\0');
            out.write(begin, p - begin);
            if(p == end)
                break;

            char const * pp = p+1;
            while(pp != end && *pp == '\0')
                ++pp;

            ptrdiff_t n = pp - p;
            while(n > 255)
            {
                out << '\0' << '\xff';
                n -= 255;
            }
            out << '\0' << char(n);
            begin = pp;
        }
        return out;
    }

} // namespace warp


//----------------------------------------------------------------------------
// ZeroUnescape
//----------------------------------------------------------------------------
class warp::ZeroUnescape : public warp::StringRange
{
public:
    explicit ZeroUnescape(strref_t str) : StringRange(str) {}
};

namespace warp {

    std::ostream & operator<<(std::ostream & out, ZeroUnescape const & x)
    {
        char const * begin = x.begin();
        char const * end = x.end();
        for(;;)
        {
            char const * p = std::find(begin, end, '\0');
            out.write(begin, p - begin);

            if(p == end)
                break;

            if(++p == end)
            {
                using namespace ex;
                raise<ValueError>("invalid zero-escaped string");
            }

            for(char i = 0; i != *p; ++i)
                out << '\0';

            begin = p + 1;
        }
        return out;
    }

} // namespace warp


#endif // WARP_ZERO_ESCAPE_H
