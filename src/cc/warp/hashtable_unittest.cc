//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2006 Josh Taylor (Kosmix Corporation)
// Created 2006-05-12
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

#include <warp/hashtable.h>
#include <unittest/main.h>
#include <iostream>

using namespace warp;
using namespace std;

BOOST_AUTO_TEST_CASE(basic)
{
    HashTable<int> m;

    cout << m.currentLoad() << ' ' << m.capacity() << endl;

    BOOST_CHECK(m.empty());
    BOOST_CHECK_EQUAL(m.size(), 0u);
    BOOST_CHECK_EQUAL(m.maxProbe(), 1u);
    BOOST_CHECK_CLOSE(m.averageProbe(), 1.0, 0.0);

    // Add something
    m.insert(1);
    BOOST_CHECK(m.contains(1));
    BOOST_CHECK_EQUAL(m.size(), 1u);
    BOOST_CHECK_EQUAL(m.maxProbe(), 1u);
    BOOST_CHECK_CLOSE(m.averageProbe(), 1.0, 0.0);

    cout << m.currentLoad() << ' ' << m.capacity() << endl;

    // Erase it
    m.erase(1);
    BOOST_CHECK(!m.contains(1));
    BOOST_CHECK_EQUAL(m.size(), 0u);
    BOOST_CHECK_EQUAL(m.maxProbe(), 1u);
    BOOST_CHECK_CLOSE(m.averageProbe(), 1.0, 0.0);

    cout << m.currentLoad() << ' ' << m.capacity() << endl;

    // Add it again
    m.insert(1);
    BOOST_CHECK(m.contains(1));
    BOOST_CHECK_EQUAL(m.size(), 1u);
    BOOST_CHECK_EQUAL(m.maxProbe(), 1u);
    BOOST_CHECK_CLOSE(m.averageProbe(), 1.0, 0.0);

    cout << m.currentLoad() << ' ' << m.capacity() << endl;

    // And another
    m.insert(1);
    BOOST_CHECK(m.contains(1));
    BOOST_CHECK_EQUAL(m.size(), 2u);
    BOOST_CHECK_EQUAL(m.maxProbe(), 2u);
    BOOST_CHECK_CLOSE(m.averageProbe(), 1.5, 1e-10);

    cout << m.currentLoad() << ' ' << m.capacity() << endl;

    // Erase one
    m.erase(1);
    BOOST_CHECK(m.contains(1));
    BOOST_CHECK_EQUAL(m.size(), 1u);
    BOOST_CHECK_EQUAL(m.maxProbe(), 2u);
    BOOST_CHECK_CLOSE(m.averageProbe(), 2.0, 0.0);

    cout << m.currentLoad() << ' ' << m.capacity() << endl;

    // And the other
    m.erase(1);
    BOOST_CHECK(!m.contains(1));
    BOOST_CHECK_EQUAL(m.size(), 0u);
    BOOST_CHECK_EQUAL(m.maxProbe(), 1u);
    BOOST_CHECK_CLOSE(m.averageProbe(), 1.0, 0.0);


    cout << m.currentLoad() << ' ' << m.capacity() << endl;

    // Add special values (empty == -1, deleted == -2) which are
    // aliased to 0.
    m.insert(-2);
    m.insert(-1);
    m.insert(0);
    m.insert(1);
    m.insert(2);
    BOOST_CHECK_EQUAL(m.size(), 5u);
    BOOST_CHECK(m.contains(-2));
    BOOST_CHECK(m.contains(-1));
    BOOST_CHECK(m.contains(0));
    BOOST_CHECK(m.contains(1));
    BOOST_CHECK(m.contains(2));

    cout << m.currentLoad() << ' ' << m.capacity() << endl;

    // Remove middle aliased value
    m.erase(-1);
    BOOST_CHECK_EQUAL(m.size(), 4u);
    BOOST_CHECK(m.contains(-2));
    BOOST_CHECK(!m.contains(-1));
    BOOST_CHECK(m.contains(0));
    BOOST_CHECK(m.contains(1));
    BOOST_CHECK(m.contains(2));

    cout << m.currentLoad() << ' ' << m.capacity() << endl;

    // Rehash and make sure things are still kosher
    m.rehash();
    BOOST_CHECK_EQUAL(m.size(), 4u);
    BOOST_CHECK(m.contains(-2));
    BOOST_CHECK(!m.contains(-1));
    BOOST_CHECK(m.contains(0));
    BOOST_CHECK(m.contains(1));
    BOOST_CHECK(m.contains(2));

    cout << m.currentLoad() << ' ' << m.capacity() << endl;

    // Clear table
    m.clear();
    BOOST_CHECK(m.empty());
    BOOST_CHECK_EQUAL(m.maxProbe(), 1u);

    cout << m.currentLoad() << ' ' << m.capacity() << endl;
}
