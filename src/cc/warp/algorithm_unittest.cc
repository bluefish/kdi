//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/warp/algorithm_unittest.cc#1 $
//
// Created 2007/06/14
//
// Copyright 2007 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#include "algorithm.h"
#include "unittest/main.h"

#include <map>
#include <utility>

using namespace warp;
using namespace ex;
using namespace std;

BOOST_AUTO_UNIT_TEST(assoc_test)
{
    map<string,string> a;
    a["x"] = "y";
    
    map<int, double> b;
    b[42] = 3.14159;

    BOOST_CHECK_EQUAL(get_assoc(a, "x"), "y");
    BOOST_CHECK_THROW(get_assoc(a, "a"), KeyError);
    BOOST_CHECK_EQUAL(get_assoc(a, "x", "z"), "y");
    BOOST_CHECK_EQUAL(get_assoc(a, "a", "b"), "b");

    BOOST_CHECK_EQUAL(get_assoc(b, 42), 3.14159);
    BOOST_CHECK_THROW(get_assoc(b, 7), KeyError);
    BOOST_CHECK_EQUAL(get_assoc(b, 42, 6.0221415e23), 3.14159);
    BOOST_CHECK_EQUAL(get_assoc(b, 7, 6.0221415e23), 6.0221415e23);
}
