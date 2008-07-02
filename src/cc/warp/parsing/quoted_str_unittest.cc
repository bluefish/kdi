//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: warp/parsing/quoted_string_unittest.cc $
//
// Created 2008/04/09
//
// Copyright 2008 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
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
