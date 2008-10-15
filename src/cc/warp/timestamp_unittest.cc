//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
// Created 2008-10-15
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

#include <warp/timestamp.h>
#include <unittest/main.h>
#include <sstream>

using namespace warp;
using namespace std;

namespace
{
    template <class T>
    string str(T const & x)
    {
        ostringstream oss;
        oss << x;
        return oss.str();
    }

    string timeStr(strref_t s)
    {
        return str(Timestamp::fromString(s));
    }
}

BOOST_AUTO_UNIT_TEST(string_conversion)
{
    BOOST_CHECK_EQUAL(str(Timestamp()), "1970-01-01T00:00:00.000000Z");
    BOOST_CHECK_EQUAL(str(Timestamp::fromSeconds(62l)), "1970-01-01T00:01:02.000000Z");
    BOOST_CHECK_EQUAL(str(Timestamp::fromMicroseconds(62030405l)), "1970-01-01T00:01:02.030405Z");

    BOOST_CHECK_EQUAL(timeStr("2008-02-20T10:52:03.787239Z"), "2008-02-20T10:52:03.787239Z");
    BOOST_CHECK_EQUAL(timeStr("2008-02-20T10:52:03.787240Z"), "2008-02-20T10:52:03.787240Z");

    BOOST_CHECK_EQUAL(timeStr("1552-11-30T22:01:33.438682Z"), "1552-11-30T22:01:33.438682Z");
    BOOST_CHECK_EQUAL(timeStr("1552-11-30t22:01:33.438682z"), "1552-11-30T22:01:33.438682Z");

    BOOST_CHECK_EQUAL(timeStr("1552-11-30t220133.438682z"),    "1552-11-30T22:01:33.438682Z");
    BOOST_CHECK_EQUAL(timeStr("1552-11-30t2201:33.438682z"),   "1552-11-30T22:01:33.438682Z");
    BOOST_CHECK_EQUAL(timeStr("1552-11-30t22:0133.438682z"),   "1552-11-30T22:01:33.438682Z");
    BOOST_CHECK_EQUAL(timeStr("1552-1130t220133.438682z"),     "1552-11-30T22:01:33.438682Z");
    BOOST_CHECK_EQUAL(timeStr("155211-30t220133.438682z"),     "1552-11-30T22:01:33.438682Z");
    BOOST_CHECK_EQUAL(timeStr("155211-30t2201:33.438682z"),    "1552-11-30T22:01:33.438682Z");
    BOOST_CHECK_EQUAL(timeStr("155211-30t22:0133.438682z"),    "1552-11-30T22:01:33.438682Z");
    BOOST_CHECK_EQUAL(timeStr("15521130t220133.438682z"),      "1552-11-30T22:01:33.438682Z");

    BOOST_CHECK_EQUAL(timeStr("1552-11-30t22:01:33.43868291z"), "1552-11-30T22:01:33.438682Z");
    BOOST_CHECK_EQUAL(timeStr("1552-11-30t22:01:33.4386821z"),  "1552-11-30T22:01:33.438682Z");
    BOOST_CHECK_EQUAL(timeStr("1552-11-30t22:01:33.438682z"),   "1552-11-30T22:01:33.438682Z");
    BOOST_CHECK_EQUAL(timeStr("1552-11-30t22:01:33.43868z"),    "1552-11-30T22:01:33.438680Z");
    BOOST_CHECK_EQUAL(timeStr("1552-11-30t22:01:33.4386z"),     "1552-11-30T22:01:33.438600Z");
    BOOST_CHECK_EQUAL(timeStr("1552-11-30t22:01:33.438z"),      "1552-11-30T22:01:33.438000Z");
    BOOST_CHECK_EQUAL(timeStr("1552-11-30t22:01:33.43z"),       "1552-11-30T22:01:33.430000Z");
    BOOST_CHECK_EQUAL(timeStr("1552-11-30t22:01:33.4z"),        "1552-11-30T22:01:33.400000Z");
    BOOST_CHECK_EQUAL(timeStr("1552-11-30t22:01:33z"),          "1552-11-30T22:01:33.000000Z");
    BOOST_CHECK_EQUAL(timeStr("1552-11-30t22:01z"),             "1552-11-30T22:01:00.000000Z");
    BOOST_CHECK_EQUAL(timeStr("1552-11-30t22z"),                "1552-11-30T22:00:00.000000Z");
    BOOST_CHECK_EQUAL(timeStr("1552-11-30z"),                   "1552-11-30T00:00:00.000000Z");
    BOOST_CHECK_EQUAL(timeStr("1552-11z"),                      "1552-11-01T00:00:00.000000Z");
    BOOST_CHECK_EQUAL(timeStr("1552z"),                         "1552-01-01T00:00:00.000000Z");
}
