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
#include <boost/preprocessor/repetition.hpp>

#ifndef WARP_LOG_MAX_PARAMS
#  define WARP_LOG_MAX_PARAMS 15
#endif

namespace warp {
    
    /// Log a line to stderr.  This function is thread-safe and will
    /// prevent threads from interleaving log messages.  The log
    /// message should not contain newlines.
    void log(strref_t msg);

    /// Log with parameter substitution for 1 to WARP_LOG_MAX_PARAMS
    /// arguments
    #undef WARP_LOG_N_PARAM
    #undef WARP_LOG_CAT_N
    #define WARP_LOG_CAT_N(z,n,data) BOOST_PP_CAT(data,n)
    #define WARP_LOG_N_PARAM(z,n,unused)                        \
                                                                \
    template <BOOST_PP_ENUM_PARAMS(n, class A)>                 \
    void log(char const * fmt,                                  \
             BOOST_PP_ENUM_BINARY_PARAMS(n, A, const & a)       \
             )                                                  \
    {                                                           \
        log((boost::format(fmt)                                 \
             BOOST_PP_REPEAT(n, WARP_LOG_CAT_N, % a)            \
            ).str());                                           \
    }
    BOOST_PP_REPEAT_FROM_TO(1, BOOST_PP_ADD(WARP_LOG_MAX_PARAMS, 1),
                            WARP_LOG_N_PARAM, ~)
    #undef WARP_LOG_N_PARAM
    #undef WARP_LOG_CAT_N

} // namespace warp

#endif // WARP_LOG_H
