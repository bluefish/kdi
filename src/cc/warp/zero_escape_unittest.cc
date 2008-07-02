//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: warp/zero_escape_unittest.cc $
//
// Created 2008/03/17
//
// Copyright 2008 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
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
