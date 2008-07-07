//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
// Created 2008-04-09
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

#include <warp/parsing/quoted_str.h>
#include <warp/strref.h>
#include <ex/exception.h>
#include <unittest/main.h>

using namespace warp;
using namespace warp::parsing;
using namespace ex;
using namespace std;
using namespace boost::spirit;

namespace {

    string p(strref_t s)
    {
        string r;
        if(parse(s.begin(), s.end(),
                 quoted_str_p[assign_a(r)] >> end_p,
                 space_p).full)
        {
            return r;
        }
        
        raise<ValueError>("parse failed: %s", s);
    }
    
}

BOOST_AUTO_UNIT_TEST(single_quote)
{
    // Empty
    BOOST_CHECK_EQUAL(p("''"), "");

    // Spacing
    BOOST_CHECK_EQUAL(p("'hello'"), "hello");
    BOOST_CHECK_EQUAL(p("'  hello  '"), "  hello  ");
    BOOST_CHECK_EQUAL(p("  '  hello  '  "), "  hello  ");
    BOOST_CHECK_EQUAL(p("  'hello'  "), "hello");

    // Escape chars
    BOOST_CHECK_EQUAL(p("'\\''"), "'");
    BOOST_CHECK_EQUAL(p("'\\\\'"), "\\");
    BOOST_CHECK_EQUAL(p("'\\n'"), "\n");
    BOOST_CHECK_EQUAL(p("'\\r'"), "\r");
    BOOST_CHECK_EQUAL(p("'\\t'"), "\t");
    BOOST_CHECK_EQUAL(p("'\\v'"), "\v");
    BOOST_CHECK_EQUAL(p("'\\x01'"), "\x01");
    BOOST_CHECK_EQUAL(p("'\\u0020'"), " ");
}

BOOST_AUTO_UNIT_TEST(double_quote)
{
    // Empty
    BOOST_CHECK_EQUAL(p("\"\""), "");

    // Spacing
    BOOST_CHECK_EQUAL(p("\"hello\""), "hello");
    BOOST_CHECK_EQUAL(p("\"  hello  \""), "  hello  ");
    BOOST_CHECK_EQUAL(p("  \"  hello  \"  "), "  hello  ");
    BOOST_CHECK_EQUAL(p("  \"hello\"  "), "hello");

    // Escape chars
    BOOST_CHECK_EQUAL(p("\"\\\"\""), "\"");
    BOOST_CHECK_EQUAL(p("\"\\\\\""), "\\");
    BOOST_CHECK_EQUAL(p("\"\\n\""), "\n");
    BOOST_CHECK_EQUAL(p("\"\\r\""), "\r");
    BOOST_CHECK_EQUAL(p("\"\\t\""), "\t");
    BOOST_CHECK_EQUAL(p("\"\\v\""), "\v");
    BOOST_CHECK_EQUAL(p("\"\\x01\""), "\x01");
    BOOST_CHECK_EQUAL(p("\"\\u0020\""), " ");
}

BOOST_AUTO_UNIT_TEST(invert_test)
{
    // Make sure we can parse every character
    for(int i = 0; i < 256; ++i)
    {
        char buf[32];
        size_t sz = snprintf(buf, sizeof(buf), "0x%02x: X", i);
        buf[sz-1] = i;
        str_data_t s = binary_data(buf, sz);
        BOOST_CHECK_EQUAL(p(reprString(s)), s);
    }
}
