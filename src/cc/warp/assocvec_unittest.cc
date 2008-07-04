//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2006 Josh Taylor (Kosmix Corporation)
// Created 2006-04-21
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

#include <warp/assocvec.h>
#include <unittest/main.h>
#include <string>
#include <iostream>
#include <map>

using namespace warp;
using namespace std;

namespace
{
    template <class C>
    void dumpMap(C x)
    {
        cout << x.size() << ": " << endl;
        for(typename C::const_iterator i = x.begin(); i != x.end(); ++i)
        {
            cout << "  '" << i->first << "' : '" << i->second << "'" << endl;
        }
    }

    template < template <class K,class V> class Map >
    void testMap()
    {
        // Do some insertions
        Map<int,int> m;
    
        BOOST_CHECK_EQUAL(m.size(), 0u);
        BOOST_CHECK(m.find(17) == m.end());
        BOOST_CHECK(m.lower_bound(10) == m.end());
    
        m[4] = 10;
        BOOST_CHECK_EQUAL(m.size(), 1u);
        BOOST_CHECK_EQUAL(m.find(4)->second, 10);
        BOOST_CHECK(m.find(4) == m.begin());
        BOOST_CHECK(m.find(17) == m.end());
    
    
        // Er... finish writing this test at some point

        m[1] = 11;
        m[7] = 12;
    
    
        m[10] = 13;
        m.erase(m.find(1));
    }

    template <class K, class V>
    class AVMap : public AssocVector<K,V> {};
}

BOOST_AUTO_TEST_CASE(test_AssocVector)
{
    testMap<AVMap>();
}


//----------------------------------------------------------------------------
// Test alternate keys
//----------------------------------------------------------------------------
namespace
{
    typedef std::pair<char *, char  *> char_span_t;
    typedef std::pair<char const *, char const *> const_char_span_t;

    struct AltKeyCmp
    {
        bool operator()(string const & a, string const & b)
        {
            return a < b;
        }
        bool operator()(string const & a, const_char_span_t const & b)
        {
            return std::lexicographical_compare(
                a.c_str(), a.c_str() + a.size(),
                b.first, b.second);
        }
        bool operator()(const_char_span_t const & a, string const & b)
        {
            return std::lexicographical_compare(
                a.first, a.second,
                b.c_str(), b.c_str() + b.size());
        }
    };
}

BOOST_AUTO_TEST_CASE(altKeyTest)
{
    AssocVector<string, int, AltKeyCmp> m;

    char const * foo = "onetwothree";
    const_char_span_t one = make_pair(foo, foo+3);
    const_char_span_t two = make_pair(one.second, one.second+3);
    const_char_span_t three = make_pair(two.second, two.second+5);

    m["one"] = 1;
    m["two"] = 2;
    m["three"] = 3;
    
    // Lookup by key type
    BOOST_CHECK_EQUAL(m.find(string("one"))->second, 1);

    // Lookup by comparable but non-convertible alternate key
    BOOST_CHECK_EQUAL(m.find(one)->second, 1);

    // Lookup by convertible but non-comparable alternate key (this
    // probably involved multiple conversions to string)
    BOOST_CHECK_EQUAL(m.find("one")->second, 1);

    // Alphabetic ordering
    BOOST_CHECK(m.find(one) < m.find(two));
    BOOST_CHECK(m.find(three) < m.find(two));
}
