//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-04-30
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

#include <warp/ticks.h>
#include <unittest/main.h>
#include <iostream>

using namespace warp;
using namespace std;

BOOST_AUTO_UNIT_TEST(tick_test)
{
    uint64_t hz = estimateTickRate();

    // Let's guess that CPU frequency should be in the range 500 MHz
    // and 5 GHz
    cout << "Got tick rate: " << hz << endl;
    BOOST_CHECK(hz >= 500000000ul);
    BOOST_CHECK(hz <= 5000000000ul);
}
