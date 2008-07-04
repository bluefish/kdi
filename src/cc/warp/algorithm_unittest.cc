//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-06-14
//
// This file is part of KDI.
// 
// KDI is free software; you can redistribute it and/or modify it
// under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// any later version.
// 
// KDI is distributed in the hope that it will be useful, but WITHOUT
// ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
// or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public
// License for more details.
// 
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
// 02110-1301, USA.
//----------------------------------------------------------------------------

#include <warp/algorithm.h>
#include <unittest/main.h>

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
