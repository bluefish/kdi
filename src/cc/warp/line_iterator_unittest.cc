//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
// Created 2008-01-19
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

#include <warp/line_iterator.h>
#include <unittest/main.h>

using namespace warp;

BOOST_AUTO_UNIT_TEST(basic)
{
    char const * TEST =
        "line 1\n"
        "line 2\r"
        "line 3\r\n"
        "\r\n"
        "\r"
        " \n"
        "\r"
        "end line";
    
    {
        // Test with stripping
        LineIterator x(TEST, strlen(TEST));
        StringRange line;
    
        BOOST_CHECK_EQUAL(x.get(line, true), true);
        BOOST_CHECK_EQUAL(line, "line 1");
        BOOST_CHECK_EQUAL(x.get(line, true), true);
        BOOST_CHECK_EQUAL(line, "line 2");
        BOOST_CHECK_EQUAL(x.get(line, true), true);
        BOOST_CHECK_EQUAL(line, "line 3");
        BOOST_CHECK_EQUAL(x.get(line, true), true);
        BOOST_CHECK_EQUAL(line, "");
        BOOST_CHECK_EQUAL(x.get(line, true), true);
        BOOST_CHECK_EQUAL(line, "");
        BOOST_CHECK_EQUAL(x.get(line, true), true);
        BOOST_CHECK_EQUAL(line, " ");
        BOOST_CHECK_EQUAL(x.get(line, true), true);
        BOOST_CHECK_EQUAL(line, "");
        BOOST_CHECK_EQUAL(x.get(line, true), true);
        BOOST_CHECK_EQUAL(line, "end line");

        // Should be EOF
        BOOST_CHECK(!x.get(line, true));

        // ... and it should stay EOF
        BOOST_CHECK(!x.get(line, true));
    }

    {
        // Test without stripping
        LineIterator x(TEST, strlen(TEST));
        StringRange line;
    
        BOOST_CHECK_EQUAL(x.get(line, false), true);
        BOOST_CHECK_EQUAL(line, "line 1\n");
        BOOST_CHECK_EQUAL(x.get(line, false), true);
        BOOST_CHECK_EQUAL(line, "line 2\r");
        BOOST_CHECK_EQUAL(x.get(line, false), true);
        BOOST_CHECK_EQUAL(line, "line 3\r\n");
        BOOST_CHECK_EQUAL(x.get(line, false), true);
        BOOST_CHECK_EQUAL(line, "\r\n");
        BOOST_CHECK_EQUAL(x.get(line, false), true);
        BOOST_CHECK_EQUAL(line, "\r");
        BOOST_CHECK_EQUAL(x.get(line, false), true);
        BOOST_CHECK_EQUAL(line, " \n");
        BOOST_CHECK_EQUAL(x.get(line, false), true);
        BOOST_CHECK_EQUAL(line, "\r");
        BOOST_CHECK_EQUAL(x.get(line, false), true);
        BOOST_CHECK_EQUAL(line, "end line");

        // Should be EOF
        BOOST_CHECK(!x.get(line, false));

        // ... and it should stay EOF
        BOOST_CHECK(!x.get(line, false));
    }
}
