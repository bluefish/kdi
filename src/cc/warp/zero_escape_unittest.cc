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

#include <warp/zero_escape.h>
#include <unittest/main.h>

using namespace warp;
using namespace ex;

BOOST_AUTO_UNIT_TEST(basic)
{
    // These test strings are strangely formatted because GCC has some
    // serious problems with embedded escapes in string literals.

    BOOST_CHECK_EQUAL(zeroEscape(""), "");
    BOOST_CHECK_EQUAL(zeroEscape("foo"), "foo");
    BOOST_CHECK_EQUAL(zeroEscape(binary_data("\0" "foo", 4)),
                      binary_data("\0" "\x01" "foo", 5));
    BOOST_CHECK_EQUAL(zeroEscape(binary_data("\0" "foo" "\0" "\0" "\0" "\0" "\0", 9)),
                      binary_data("\0" "\x01" "foo" "\0" "\x05", 7));

    BOOST_CHECK_EQUAL(zeroUnescape(""), "");
    BOOST_CHECK_EQUAL(zeroUnescape("foo"), "foo");
    BOOST_CHECK_EQUAL(zeroUnescape(binary_data("\0" "\x01" "foo", 5)),
                      binary_data("\0" "foo", 4));
    BOOST_CHECK_EQUAL(zeroUnescape(binary_data("\0" "\x01" "foo" "\0" "\x05", 7)),
                      binary_data("\0" "foo" "\0" "\0" "\0" "\0" "\0", 9));

    BOOST_CHECK_THROW(zeroUnescape(binary_data("foo" "\0", 4)), ValueError);
}
