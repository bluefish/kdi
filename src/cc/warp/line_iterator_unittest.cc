//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: line_iterator_unittest.cc $
//
// Created 2008/01/19
//
// Copyright 2008 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
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
        str_data_t line;
    
        BOOST_CHECK(x.get(line, true) && line == "line 1");
        BOOST_CHECK(x.get(line, true) && line == "line 2");
        BOOST_CHECK(x.get(line, true) && line == "line 3");
        BOOST_CHECK(x.get(line, true) && line == "");
        BOOST_CHECK(x.get(line, true) && line == "");
        BOOST_CHECK(x.get(line, true) && line == " ");
        BOOST_CHECK(x.get(line, true) && line == "");
        BOOST_CHECK(x.get(line, true) && line == "end line");

        // Should be EOF
        BOOST_CHECK(!x.get(line, true));

        // ... and it should stay EOF
        BOOST_CHECK(!x.get(line, true));
    }

    {
        // Test without stripping
        LineIterator x(TEST, strlen(TEST));
        str_data_t line;
    
        BOOST_CHECK(x.get(line, false) && line == "line 1\n");
        BOOST_CHECK(x.get(line, false) && line == "line 2\r");
        BOOST_CHECK(x.get(line, false) && line == "line 3\r\n");
        BOOST_CHECK(x.get(line, false) && line == "\r\n");
        BOOST_CHECK(x.get(line, false) && line == "\r");
        BOOST_CHECK(x.get(line, false) && line == " \n");
        BOOST_CHECK(x.get(line, false) && line == "\r");
        BOOST_CHECK(x.get(line, false) && line == "end line");

        // Should be EOF
        BOOST_CHECK(!x.get(line, false));

        // ... and it should stay EOF
        BOOST_CHECK(!x.get(line, false));
    }
}
