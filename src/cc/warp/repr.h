//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-01-26
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

#ifndef WARP_REPR_H
#define WARP_REPR_H

#include <warp/string_range.h>
#include <iostream>

namespace warp {

    class ReprEscape
        : public StringRange
    {
    public:
        explicit ReprEscape(strref_t s) : StringRange(s) {}
    };

    inline std::ostream & operator<<(std::ostream & out, ReprEscape const & x)
    {
        char const * HEX = "0123456789abcdef";
        char buf[1024];
        char const * p_end = x.end();
        char * q_end = buf + sizeof(buf) - 4;
        
        char * q = buf;
        for(char const * p = x.begin(); p != p_end; ++p)
        {
            if(*p <= '~')
            {
                if(*p >= ' ')
                {
                    switch(*p)
                    {
                        case '"':  *q++ = '\\'; *q++ = '"';  break;
                        case '\\': *q++ = '\\'; *q++ = '\\'; break;
                        default:
                            *q++ = *p;
                    }
                }
                else
                {
                    switch(*p)
                    {
                        case '\n': *q++ = '\\'; *q++ = 'n';  break;
                        case '\t': *q++ = '\\'; *q++ = 't';  break;
                        case '\r': *q++ = '\\'; *q++ = 'r';  break;
                        case '\b': *q++ = '\\'; *q++ = 'b';  break;
                        case '\a': *q++ = '\\'; *q++ = 'a';  break;
                        case '\f': *q++ = '\\'; *q++ = 'f';  break;
                        case '\v': *q++ = '\\'; *q++ = 'v';  break;
                        default:
                            *q++ = '\\'; *q++ = 'x';
                            *q++ = HEX[(*p >> 4) & 0x0f];
                            *q++ = HEX[*p & 0x0f];
                    }
                }
            }
            else
            {
                *q++ = '\\'; *q++ = 'x';
                *q++ = HEX[(*p >> 4) & 0x0f];
                *q++ = HEX[*p & 0x0f];
            }

            if(q >= q_end)
            {
                out.write(buf, q - buf);
                q = buf;
            }
        }

        if(q != buf)
            out.write(buf, q - buf);
        return out;
    }

} // namespace warp

#endif // WARP_REPR_H
