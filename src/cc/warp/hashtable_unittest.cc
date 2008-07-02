//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/warp/hashtable_unittest.cc#1 $
//
// Created 2006/05/12
//
// Copyright 2006 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#include "hashtable.h"
#include "unittest/main.h"
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
