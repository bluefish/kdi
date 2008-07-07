//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-03-23
// 
// This file is part of the unittest library.
// 
// The unittest library is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License as published
// by the Free Software Foundation; either version 2 of the License, or any
// later version.
// 
// The unittest library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General
// Public License for more details.
// 
// You should have received a copy of the GNU General Public License along
// with this program; if not, write to the Free Software Foundation, Inc.,
// 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.
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
