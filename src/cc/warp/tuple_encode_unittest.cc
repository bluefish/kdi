//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
// Created 2008-03-17
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

#include <warp/tuple_encode.h>
#include <unittest/main.h>

using namespace warp;
using namespace ex;
using namespace std;

BOOST_AUTO_UNIT_TEST(strseq)
{
    str_data_t const DATA[] = {
        binary_data("", 0),
        binary_data("llama", 5),
        binary_data("\0" "cat" "\0" "\0", 6),
        binary_data("", 0),
        binary_data("", 0),
        binary_data("\0", 1),
        binary_data("stuff" "\0" "\0" "\0" "\0" "\0", 10),
    };
    size_t const N = sizeof(DATA) / sizeof(*DATA);

    for(size_t i = 0; i < N; ++i)
    {
        for(size_t j = i; j < N; ++j)
        {
            vector<str_data_t> t(DATA+i, DATA+j);
            string s = encodeStringSequence(t);
            vector<string> d = decodeStringSequence(s);
            
            BOOST_CHECK_EQUAL_COLLECTIONS(
                t.begin(), t.end(), d.begin(), d.end());
        }
    }
}

BOOST_AUTO_UNIT_TEST(tuple_usage)
{
    string x = encodeTuple(tie("foo", "bar", "baz"));
    
    string a,b,c;
    decodeTuple(x, tie(a,b,c));

    BOOST_CHECK_EQUAL(a, "foo");
    BOOST_CHECK_EQUAL(b, "bar");
    BOOST_CHECK_EQUAL(c, "baz");

    x = encodeTuple(tie(c,b,a,"llama"));

    string d;
    decodeTuple(x, tie(a,b,c,d));

    BOOST_CHECK_EQUAL(a, "baz");
    BOOST_CHECK_EQUAL(b, "bar");
    BOOST_CHECK_EQUAL(c, "foo");
    BOOST_CHECK_EQUAL(d, "llama");

    string empty = encodeTuple(make_tuple());
    decodeTuple(empty, make_tuple());
}

BOOST_AUTO_UNIT_TEST(tuple_order)
{
    BOOST_CHECK(encodeTuple(make_tuple()) < encodeTuple(make_tuple("")));
    BOOST_CHECK(encodeTuple(make_tuple("a")) < encodeTuple(make_tuple("b")));
    BOOST_CHECK(encodeTuple(make_tuple("a")) < encodeTuple(make_tuple("a","")));
    BOOST_CHECK(encodeTuple(make_tuple("a","z")) <
                encodeTuple(make_tuple("ab","")));
    BOOST_CHECK(encodeTuple(make_tuple("a","z","x","q","r")) <
                encodeTuple(make_tuple("a","z","y","q","r")));
    BOOST_CHECK(encodeTuple(make_tuple("aa","z","x","q","r")) <
                encodeTuple(make_tuple("b","z","x","q","r")));
}

BOOST_AUTO_UNIT_TEST(tuple_throw)
{
    string a,b,c;

    BOOST_CHECK_THROW(decodeTuple(encodeTuple(make_tuple("1","2")), tie(a)), ValueError);
    BOOST_CHECK_THROW(decodeTuple(encodeTuple(make_tuple("1","2")), tie(a,b,c)), ValueError);
    BOOST_CHECK_THROW(decodeTuple(encodeTuple(make_tuple()), tie(a)), ValueError);
    BOOST_CHECK_THROW(decodeTuple(encodeTuple(make_tuple("1")), make_tuple()), ValueError);
}
