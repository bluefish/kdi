//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
// Created 2008-02-28
// 
// This file is part of KDI.
// 
// KDI is free software; you can redistribute it and/or modify it under the
// terms of the GNU General Public License as published by the Free Software
// Foundation; either version 2 of the License, or any later version.
// 
// KDI is distributed in the hope that it will be useful, but WITHOUT ANY
// WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
// FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
// details.
// 
// You should have received a copy of the GNU General Public License along
// with this program; if not, write to the Free Software Foundation, Inc.,
// 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.
//----------------------------------------------------------------------------

#include <kdi/scan_predicate.h>
#include <warp/string_range.h>
#include <ex/exception.h>
#include <unittest/main.h>
#include <stdlib.h>
#include <sstream>
#include <string>

using namespace kdi;
using namespace warp;
using namespace std;
using namespace ex;

namespace {

    /// Parse expression as a ScanPredicate and output it as a string
    string p(strref_t expr)
    {
        ostringstream oss;
        oss << ScanPredicate(expr);
        return oss.str();
    }

}

BOOST_AUTO_UNIT_TEST(parse_test)
{
    // Set TZ to UTC so the Timestamps print in a consistent way
    setenv("TZ", "UTC", 1);

    // Empty predicates
    BOOST_CHECK_EQUAL(p(""), "");
    BOOST_CHECK_EQUAL(p("   "), "");

    // Row predicates
    BOOST_CHECK_EQUAL(p("  row < 'foo'  "), "row < \"foo\"");
    BOOST_CHECK_EQUAL(p("row ~= 'foo'"), "\"foo\" <= row < \"fop\"");
    BOOST_CHECK_EQUAL(p("row ~= 'foo\\xff'"), "\"foo\\xff\" <= row < \"fop\"");
    BOOST_CHECK_EQUAL(p("row ~= ''"), "row >= \"\"");

    // Documentation examples
    BOOST_CHECK_EQUAL(p("row = 'com.foo.www/index.html' and history = 1"),
                      "row = \"com.foo.www/index.html\" and history = 1");
    BOOST_CHECK_EQUAL(p("row ~= 'com.foo' and time >= 1999-01-02T03:04:05.678901Z"),
                      "\"com.foo\" <= row < \"com.fop\" and time >= 1999-01-02T03:04:05.678901Z");
    BOOST_CHECK_EQUAL(p("\"word:cat\" < column <= \"word:dog\" or column >= \"word:fish\""),
                      "\"word:cat\" < column <= \"word:dog\" or column >= \"word:fish\"");
    BOOST_CHECK_EQUAL(p("time = @0"), "time = @0");

    // Check trailing slash
    BOOST_CHECK_EQUAL(p("row = 'foo\\\\'"), "row = \"foo\\\\\"");
    BOOST_CHECK_THROW(p("row = 'foo\\'"), ValueError);

    // Check basic escapes
    BOOST_CHECK_EQUAL(p("row ~= '\\x00'"), "\"\\x00\" <= row < \"\\x01\"");
    BOOST_CHECK_EQUAL(p("'com.v\\xe0' <= row < 'com.xp'"), "\"com.v\\xe0\" <= row < \"com.xp\"");

    // This is hardly an exhaustive test suite...
}

namespace {
    
    std::string clipRow(strref_t expr, char const * lb, char const * ub)
    {
        Interval<string> span;
        span.setInfinite();
        if(lb)
            span.setLowerBound(string(lb));
        if(ub)
            span.setUpperBound(string(ub));
        
        ostringstream oss;
        oss << ScanPredicate(expr).clipRows(span);
        return oss.str();
    }

}

BOOST_AUTO_UNIT_TEST(clip_test)
{
    BOOST_CHECK_EQUAL(clipRow("", "bar", "foo"), "\"bar\" <= row < \"foo\"");
    BOOST_CHECK_EQUAL(clipRow("row > 'cat'", "bar", "foo"), "\"cat\" < row < \"foo\"");
    BOOST_CHECK_EQUAL(clipRow("row < 'cat' and history = 3", "bar", "foo"), "\"bar\" <= row < \"cat\" and history = 3");

    BOOST_CHECK_EQUAL(clipRow("row < 'cat'", 0, 0), "row < \"cat\"");
    BOOST_CHECK_EQUAL(clipRow("row > 'cat'", 0, 0), "row > \"cat\"");
    BOOST_CHECK_EQUAL(clipRow("row > 'cat'", 0, "dog"), "\"cat\" < row < \"dog\"");
    BOOST_CHECK_EQUAL(clipRow("row < 'rat'", "dog", 0), "\"dog\" <= row < \"rat\"");

    BOOST_CHECK_EQUAL(clipRow("row < 'cat'", "dog", 0), "\"\" < row < \"\"");
}

namespace {
    bool testColumnFamily(strref_t expr, size_t num_families) {
        std::vector<warp::StringRange> families;
        ScanPredicate pred(expr);
        pred.getColumnFamilies(families);
        return num_families == families.size();
    }
}

BOOST_AUTO_UNIT_TEST(column_family_test)
{
    BOOST_CHECK(testColumnFamily("", 0));
    BOOST_CHECK(testColumnFamily("column = 'source:whitelist'", 1));
    BOOST_CHECK(testColumnFamily("column = 'source:whitelist' or column = 'source:deepcrawl'", 1));
    BOOST_CHECK(testColumnFamily("column = 'source:whitelist' or column = 'depth:1'", 2));
    BOOST_CHECK(testColumnFamily("column ~= 'source:deepcrawl'", 1));
    BOOST_CHECK(testColumnFamily("column ~= 'source:'", 1));
    BOOST_CHECK(testColumnFamily("column ~= 'source'", 0));
    BOOST_CHECK(testColumnFamily("column < 'source;'", 0));
    BOOST_CHECK(testColumnFamily("'source:' < column < 'source;'", 1));
    BOOST_CHECK(testColumnFamily("'source:a' < column < 'source:d'", 1));
    BOOST_CHECK(testColumnFamily("'source:a' <= column < 'source:d'", 1));
    BOOST_CHECK(testColumnFamily("'source:a' < column <= 'source:d'", 1));
    BOOST_CHECK(testColumnFamily("'source:a' <= column <= 'source:d'", 1));
    BOOST_CHECK(testColumnFamily("'source1:a' <= column <= 'source2:d'", 0));
    BOOST_CHECK(testColumnFamily("column = 'source:whitelist' or column > 'source:whitelist'", 0));
    BOOST_CHECK(testColumnFamily("column = 'source:whitelist' or column > 'zeta'", 0));
}
