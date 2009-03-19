//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-03-18
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

#include <kdi/server/name_util.h>
#include <unittest/main.h>
#include <sstream>

using namespace kdi;
using namespace kdi::server;

using std::string;
using namespace warp;

namespace {

    string decode(strref_t name)
    {
        string table;
        IntervalPoint<string> last;
        
        decodeTabletName(name, table, last);

        std::ostringstream oss;
        oss << '(' << table << "):" << last;
        return oss.str();
    }

    string encode(strref_t table, IntervalPoint<string> const & last)
    {
        string tablet;
        encodeTabletName(table, last, tablet);
        return tablet;
    }
    
    IntervalPoint<string> pt(string const & v, PointType t)
    {
        return IntervalPoint<string>(v,t);
    }

    IntervalPoint<string> pt(PointType t)
    {
        return IntervalPoint<string>(string(),t);
    }

}

BOOST_AUTO_UNIT_TEST(tablet_decode)
{
    BOOST_CHECK_EQUAL(decode("hi there"), "(hi):there]");
    BOOST_CHECK_EQUAL(decode("hi!"), "(hi):>>");
    BOOST_CHECK_EQUAL(decode("foo/bar baz!"), "(foo/bar):baz!]");
    BOOST_CHECK_EQUAL(decode("foo/bar!"), "(foo/bar):>>");
    BOOST_CHECK_EQUAL(decode("HI there"), "(HI):there]");
    BOOST_CHECK_EQUAL(decode("abcABC123_/-xyzXYZ890   !"), "(abcABC123_/-xyzXYZ890):  !]");

    BOOST_CHECK_THROW(decode("hi.there!"), BadTabletNameError);
    BOOST_CHECK_THROW(decode("hi"), BadTabletNameError);
    BOOST_CHECK_THROW(decode("!"), BadTabletNameError);
    BOOST_CHECK_THROW(decode(" hi"), BadTabletNameError);
    BOOST_CHECK_THROW(decode("hi! "), BadTabletNameError);
}

BOOST_AUTO_UNIT_TEST(tablet_encode)
{
    BOOST_CHECK_EQUAL(encode("bwaka", pt(PT_INFINITE_UPPER_BOUND)), "bwaka!");
    BOOST_CHECK_EQUAL(encode("test", pt("xyz", PT_INFINITE_UPPER_BOUND)), "test!");
    BOOST_CHECK_EQUAL(encode("FROMAGE/con_queso", pt("with cheese", PT_INCLUSIVE_UPPER_BOUND)), "FROMAGE/con_queso with cheese");

    BOOST_CHECK_THROW(encode("", pt(PT_INFINITE_UPPER_BOUND)), BadTabletNameError);
    BOOST_CHECK_THROW(encode(" x", pt(PT_INFINITE_UPPER_BOUND)), BadTabletNameError);
    BOOST_CHECK_THROW(encode("x ", pt(PT_INFINITE_UPPER_BOUND)), BadTabletNameError);
    BOOST_CHECK_THROW(encode("test", pt(PT_INFINITE_LOWER_BOUND)), BadTabletNameError);
    BOOST_CHECK_THROW(encode("test", pt("bing", PT_INCLUSIVE_LOWER_BOUND)), BadTabletNameError);
    BOOST_CHECK_THROW(encode("test", pt("bing", PT_EXCLUSIVE_LOWER_BOUND)), BadTabletNameError);
    BOOST_CHECK_THROW(encode("test", pt("bing", PT_EXCLUSIVE_UPPER_BOUND)), BadTabletNameError);
}
