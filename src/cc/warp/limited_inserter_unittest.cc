//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: warp/limited_inserter_unittest.cc $
//
// Created 2008/03/17
//
// Copyright 2008 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#include <warp/limited_inserter.h>
#include <unittest/main.h>
#include <algorithm>
#include <vector>

using namespace warp;
using namespace std;

BOOST_AUTO_UNIT_TEST(basic)
{
    int BUF[] = { 0, 1, 2, 3 };
    int * result;

    result = fill_n(limited_inserter(BUF, BUF+4), 0, 42);
    BOOST_CHECK(result == BUF);
    BOOST_CHECK_EQUAL(BUF[0], 0);
    BOOST_CHECK_EQUAL(BUF[1], 1);
    BOOST_CHECK_EQUAL(BUF[2], 2);
    BOOST_CHECK_EQUAL(BUF[3], 3);

    result = fill_n(limited_inserter(BUF, BUF+4), 1, 42);
    BOOST_CHECK(result == BUF+1);
    BOOST_CHECK_EQUAL(BUF[0], 42);
    BOOST_CHECK_EQUAL(BUF[1], 1);
    BOOST_CHECK_EQUAL(BUF[2], 2);
    BOOST_CHECK_EQUAL(BUF[3], 3);

    BOOST_CHECK_THROW(fill_n(limited_inserter(BUF, BUF+3), 6, 17), out_of_range);
    BOOST_CHECK_EQUAL(BUF[0], 17);
    BOOST_CHECK_EQUAL(BUF[1], 17);
    BOOST_CHECK_EQUAL(BUF[2], 17);
    BOOST_CHECK_EQUAL(BUF[3], 3);

    result = fill_n(limited_inserter(BUF, BUF+4), 4, 99);
    BOOST_CHECK(result == BUF+4);
    BOOST_CHECK_EQUAL(BUF[0], 99);
    BOOST_CHECK_EQUAL(BUF[1], 99);
    BOOST_CHECK_EQUAL(BUF[2], 99);
    BOOST_CHECK_EQUAL(BUF[3], 99);
}

BOOST_AUTO_UNIT_TEST(basic_vec)
{
    vector<int> buf(4);
    buf[0] = 0;
    buf[1] = 1;
    buf[2] = 2;
    buf[3] = 3;
    vector<int>::iterator result;

    result = fill_n(limited_inserter(buf.begin(), buf.end()), 0, 42);
    BOOST_CHECK(result == buf.begin());
    BOOST_CHECK_EQUAL(buf[0], 0);
    BOOST_CHECK_EQUAL(buf[1], 1);
    BOOST_CHECK_EQUAL(buf[2], 2);
    BOOST_CHECK_EQUAL(buf[3], 3);

    result = fill_n(limited_inserter(buf.begin(), buf.end()), 1, 42);
    BOOST_CHECK(result == buf.begin()+1);
    BOOST_CHECK_EQUAL(buf[0], 42);
    BOOST_CHECK_EQUAL(buf[1], 1);
    BOOST_CHECK_EQUAL(buf[2], 2);
    BOOST_CHECK_EQUAL(buf[3], 3);

    BOOST_CHECK_THROW(fill_n(limited_inserter(buf.begin(), buf.end()-1), 6, 17), out_of_range);
    BOOST_CHECK_EQUAL(buf[0], 17);
    BOOST_CHECK_EQUAL(buf[1], 17);
    BOOST_CHECK_EQUAL(buf[2], 17);
    BOOST_CHECK_EQUAL(buf[3], 3);

    result = fill_n(limited_inserter(buf.begin(), buf.end()), 4, 99);
    BOOST_CHECK(result == buf.end());
    BOOST_CHECK_EQUAL(buf[0], 99);
    BOOST_CHECK_EQUAL(buf[1], 99);
    BOOST_CHECK_EQUAL(buf[2], 99);
    BOOST_CHECK_EQUAL(buf[3], 99);
}
