//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/unittest/main.h#1 $
//
// Created 2007/03/23
//
// Copyright 2007 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef UNITTEST_MAIN_H
#define UNITTEST_MAIN_H

// This provides main().  Implement test functions like this:
//
// BOOST_AUTO_TEST_CASE(some_test)
// {
//     BOOST_CHECK_THROW(raise<RuntimeError>("Boom!"), RuntimeError);
//     BOOST_CHECK_EQUAL(2+2, 4);
//     BOOST_CHECK_SMALL(1.0 / 1e50);
// }

#include <boost/version.hpp>
#if ((BOOST_VERSION/100)%1000) < 34
#define BOOST_AUTO_TEST_MAIN
#else
#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK
#endif

#include <boost/test/auto_unit_test.hpp>
#include <boost/test/floating_point_comparison.hpp>
#include <boost/test/unit_test_suite.hpp>

#ifndef BOOST_AUTO_UNIT_TEST
// @todo TODO Update use of macro name for Boost 1.33+.
#define BOOST_AUTO_UNIT_TEST BOOST_AUTO_TEST_CASE
#endif

#endif // UNITTEST_MAIN_H
