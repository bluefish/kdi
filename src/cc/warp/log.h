//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
// Created 2008-03-11
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

#ifndef WARP_LOG_H
#define WARP_LOG_H

#include <warp/string_range.h>
#include <boost/format.hpp>

namespace warp {
    
    /// Log a line to stderr.  This function is thread-safe and will
    /// prevent threads from interleaving log messages.  The log
    /// message should not contain newlines.
    void log(strref_t msg);

    /// Log a line with argument substitution.
    template <class A1>
    void log(char const * fmt, A1 const & a1)
    {
        log((boost::format(fmt) % a1).str());
    }

    /// Log a line with argument substitution.
    template <class A1, class A2>
    void log(char const * fmt, A1 const & a1, A2 const & a2)
    {
        log((boost::format(fmt) % a1 % a2).str());
    }

    /// Log a line with argument substitution.
    template <class A1, class A2, class A3>
    void log(char const * fmt, A1 const & a1, A2 const & a2, A3 const & a3)
    {
        log((boost::format(fmt) % a1 % a2 % a3).str());
    }

    /// Log a line with argument substitution.
    template <class A1, class A2, class A3, class A4>
    void log(char const * fmt, A1 const & a1, A2 const & a2, A3 const & a3,
             A4 const & a4)
    {
        log((boost::format(fmt) % a1 % a2 % a3 % a4).str());
    }

    /// Log a line with argument substitution.
    template <class A1, class A2, class A3, class A4, class A5>
    void log(char const * fmt, A1 const & a1, A2 const & a2, A3 const & a3,
             A4 const & a4, A5 const & a5)
    {
        log((boost::format(fmt) % a1 % a2 % a3 % a4 % a5).str());
    }

    /// Log a line with argument substitution.
    template <class A1, class A2, class A3, class A4, class A5, class A6>
    void log(char const * fmt, A1 const & a1, A2 const & a2, A3 const & a3,
             A4 const & a4, A5 const & a5, A6 const & a6)
    {
        log((boost::format(fmt) % a1 % a2 % a3 % a4 % a5 % a6).str());
    }

    /// Log a line with argument substitution.
    template <class A1, class A2, class A3, class A4, class A5, class A6,
              class A7>
    void log(char const * fmt, A1 const & a1, A2 const & a2, A3 const & a3,
             A4 const & a4, A5 const & a5, A6 const & a6, A7 const & a7)
    {
        log((boost::format(fmt) % a1 % a2 % a3 % a4 % a5 % a6 % a7).str());
    }

    /// Log a line with argument substitution.
    template <class A1, class A2, class A3, class A4, class A5, class A6,
              class A7, class A8>
    void log(char const * fmt, A1 const & a1, A2 const & a2, A3 const & a3,
             A4 const & a4, A5 const & a5, A6 const & a6, A7 const & a7,
             A8 const & a8)
    {
        log((boost::format(fmt) % a1 % a2 % a3 % a4 % a5 % a6 % a7 % a8).str());
    }

    /// Log a line with argument substitution.
    template <class A1, class A2, class A3, class A4, class A5, class A6,
              class A7, class A8, class A9>
    void log(char const * fmt, A1 const & a1, A2 const & a2, A3 const & a3,
             A4 const & a4, A5 const & a5, A6 const & a6, A7 const & a7,
             A8 const & a8, A9 const & a9)
    {
        log((boost::format(fmt) % a1 % a2 % a3 % a4 % a5 % a6 % a7 % a8 % a9).str());
    }

} // namespace warp

#endif // WARP_LOG_H
