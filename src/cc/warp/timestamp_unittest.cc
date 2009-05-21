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
#include <warp/EnvironmentVariable.h>
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

    string usecStr(int64_t us)
    {
        return str(Timestamp::fromMicroseconds(us));
    }

    string timeStr(strref_t s)
    {
        return str(Timestamp::fromString(s));
    }
}

BOOST_AUTO_UNIT_TEST(timestamp_set)
{
    Timestamp t;

    // at Epoch, UTC
    t.set(1970, 1, 1, 0, 0, 0, 0, 0);
    BOOST_CHECK_EQUAL(t.toMicroseconds(), 0);

    // 1 microsecond after Epoch, UTC
    t.set(1970, 1, 1, 0, 0, 0, 1, 0);
    BOOST_CHECK_EQUAL(t.toMicroseconds(), 1);

    // 1 second after Epoch, UTC
    t.set(1970, 1, 1, 0, 0, 1, 0, 0);
    BOOST_CHECK_EQUAL(t.toMicroseconds(), 1000000);

    // gmtoff = -1 sec
    t.set(1970, 1, 1, 0, 0, 0, 0, -1);
    BOOST_CHECK_EQUAL(t.toMicroseconds(), 1000000);

    // 2008-11-16 11:05:08.204113 PST (-0800)
    t.set(2008, 11, 19, 11, 5, 8, 204113, -8*60*60);
    BOOST_CHECK_EQUAL(t.toMicroseconds(), 1227121508204113);

    // 2008-11-16 12:05:08.204113 "PDT" (-0700)
    t.set(2008, 11, 19, 12, 5, 8, 204113, -7*60*60);
    BOOST_CHECK_EQUAL(t.toMicroseconds(), 1227121508204113);
}

BOOST_AUTO_UNIT_TEST(timestamp_set_local_eastern)
{
    // Test EST/EDT
    EnvironmentVariable env_TZ("TZ", "EST+5EDT,M4.1.0/2,M10.5.0/2");
    Timestamp t;

    // 1970-01-01 00:00:00 EST (-0500)
    t.setLocal(1970, 1, 1, 0, 0, 0, 0);
    BOOST_CHECK_EQUAL(t.toMicroseconds(), 5*60*60*1000000L);

    // 2004-06-15 14:35:06 EDT (-0400)
    t.setLocal(2004, 6, 15, 14, 35, 6, 0);
    BOOST_CHECK_EQUAL(t.toMicroseconds(), 1087324506000000);

    // 1963-02-03 05:07:59.1 EST (-0500)
    t.setLocal(1963, 2, 3, 5, 7, 59, 100000);
    BOOST_CHECK_EQUAL(t.toMicroseconds(), -218037120900000);
}

BOOST_AUTO_UNIT_TEST(timestamp_set_eastern)
{
    // Test EST/EDT
    Timestamp t;

    // 1970-01-01 00:00:00 EST (-0500)
    t.set(1970, 1, 1, 0, 0, 0, 0, -5*60*60);
    BOOST_CHECK_EQUAL(t.toMicroseconds(), 5*60*60*1000000L);

    // 2004-06-15 14:35:06 EDT (-0400)
    t.set(2004, 6, 15, 14, 35, 6, 0, -4*60*60);
    BOOST_CHECK_EQUAL(t.toMicroseconds(), 1087324506000000);

    // 1963-02-03 05:07:59.1 EST (-0500)
    t.set(1963, 2, 3, 5, 7, 59, 100000, -5*60*60);
    BOOST_CHECK_EQUAL(t.toMicroseconds(), -218037120900000);
}

BOOST_AUTO_UNIT_TEST(timestamp_set_local_pacific)
{
    // Test PST/PDT
    EnvironmentVariable env_TZ("TZ", "PST+8PDT,M4.1.0/2,M10.5.0/2");
    Timestamp t;

    // 1970-01-01 00:00:00 PST (-0800)
    t.setLocal(1970, 1, 1, 0, 0, 0, 0);
    BOOST_CHECK_EQUAL(t.toMicroseconds(), 8*60*60*1000000L);
        
    // 2004-06-15 11:35:06 PDT (-0700)
    t.setLocal(2004, 6, 15, 11, 35, 6, 0);
    BOOST_CHECK_EQUAL(t.toMicroseconds(), 1087324506000000);

    // 1963-02-03 02:07:59.1 PST (-0800)
    t.setLocal(1963, 2, 3, 2, 7, 59, 100000);
    BOOST_CHECK_EQUAL(t.toMicroseconds(), -218037120900000);
}

BOOST_AUTO_UNIT_TEST(timestamp_set_pacific)
{
    // Test PST/PDT
    Timestamp t;

    // 1970-01-01 00:00:00 PST (-0800)
    t.set(1970, 1, 1, 0, 0, 0, 0, -8*60*60);
    BOOST_CHECK_EQUAL(t.toMicroseconds(), 8*60*60*1000000L);
        
    // 2004-06-15 11:35:06 PDT (-0700)
    t.set(2004, 6, 15, 11, 35, 6, 0, -7*60*60);
    BOOST_CHECK_EQUAL(t.toMicroseconds(), 1087324506000000);

    // 1963-02-03 02:07:59.1 PST (-0800)
    t.set(1963, 2, 3, 2, 7, 59, 100000, -8*60*60);
    BOOST_CHECK_EQUAL(t.toMicroseconds(), -218037120900000);
}

BOOST_AUTO_UNIT_TEST(timestamp_set_local_utc)
{
    // Test UTC
    EnvironmentVariable env_TZ("TZ", "UTC");
    Timestamp t;

    // 1970-01-01 00:00:00 UTC
    t.setLocal(1970, 1, 1, 0, 0, 0, 0);
    BOOST_CHECK_EQUAL(t.toMicroseconds(), 0);
        
    // 2004-06-15 18:35:06 UTC
    t.setLocal(2004, 6, 15, 18, 35, 6, 0);
    BOOST_CHECK_EQUAL(t.toMicroseconds(), 1087324506000000);

    // 1963-02-03 10:07:59.1 UTC
    t.setLocal(1963, 2, 3, 10, 7, 59, 100000);
    BOOST_CHECK_EQUAL(t.toMicroseconds(), -218037120900000);
}

BOOST_AUTO_UNIT_TEST(timestamp_set_utc)
{
    // Test UTC
    Timestamp t;

    // 1970-01-01 00:00:00 UTC
    t.setUtc(1970, 1, 1, 0, 0, 0, 0);
    BOOST_CHECK_EQUAL(t.toMicroseconds(), 0);
        
    // 2004-06-15 18:35:06 UTC
    t.setUtc(2004, 6, 15, 18, 35, 6, 0);
    BOOST_CHECK_EQUAL(t.toMicroseconds(), 1087324506000000);

    // 1963-02-03 10:07:59.1 UTC
    t.setUtc(1963, 2, 3, 10, 7, 59, 100000);
    BOOST_CHECK_EQUAL(t.toMicroseconds(), -218037120900000);
}

BOOST_AUTO_UNIT_TEST(string_format_utc)
{
    EnvironmentVariable env_TZ("TZ", "UTC");

    BOOST_CHECK_EQUAL(usecStr(0L),               "1970-01-01T00:00:00.000000Z");
    BOOST_CHECK_EQUAL(usecStr(1087324506000000), "2004-06-15T18:35:06.000000Z");
    BOOST_CHECK_EQUAL(usecStr(-218037120900000), "1963-02-03T10:07:59.100000Z");
}

BOOST_AUTO_UNIT_TEST(string_format_pacific)
{
    EnvironmentVariable env_TZ("TZ", "PST+8PDT,M4.1.0/2,M10.5.0/2");

    BOOST_CHECK_EQUAL(usecStr(0L),               "1969-12-31T16:00:00.000000-08");
    BOOST_CHECK_EQUAL(usecStr(1087324506000000), "2004-06-15T11:35:06.000000-07");
    BOOST_CHECK_EQUAL(usecStr(-218037120900000), "1963-02-03T02:07:59.100000-08");
}

BOOST_AUTO_UNIT_TEST(string_format_eastern)
{
    EnvironmentVariable env_TZ("TZ", "EST+5EDT,M4.1.0/2,M10.5.0/2");

    BOOST_CHECK_EQUAL(usecStr(0L),               "1969-12-31T19:00:00.000000-05");
    BOOST_CHECK_EQUAL(usecStr(1087324506000000), "2004-06-15T14:35:06.000000-04");
    BOOST_CHECK_EQUAL(usecStr(-218037120900000), "1963-02-03T05:07:59.100000-05");
}

BOOST_AUTO_UNIT_TEST(string_format_minute_tz)
{
    EnvironmentVariable env_TZ("TZ", "XYZ-00:30");

    BOOST_CHECK_EQUAL(usecStr(0L),               "1970-01-01T00:30:00.000000+0030");
    BOOST_CHECK_EQUAL(usecStr(1087324506000000), "2004-06-15T19:05:06.000000+0030");
    BOOST_CHECK_EQUAL(usecStr(-218037120900000), "1963-02-03T10:37:59.100000+0030");
}

BOOST_AUTO_UNIT_TEST(string_format_subminute_tz)
{
    EnvironmentVariable env_TZ("TZ", "XYZ-00:30:01");

    // Cannot be represented as ISO8601.  Fall back to UTC...
    BOOST_CHECK_EQUAL(usecStr(0L),               "1970-01-01T00:00:00.000000Z");
    BOOST_CHECK_EQUAL(usecStr(1087324506000000), "2004-06-15T18:35:06.000000Z");
    BOOST_CHECK_EQUAL(usecStr(-218037120900000), "1963-02-03T10:07:59.100000Z");
}

BOOST_AUTO_UNIT_TEST(string_parse)
{
    EnvironmentVariable env_TZ("TZ", "UTC");

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
