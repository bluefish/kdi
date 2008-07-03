//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-07-21
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

#include "unittest/main.h"
#include "warp/heap.h"
#include <limits>
#include <functional>

using namespace warp;

namespace
{
    size_t const K = 10;
    size_t const N = 20;
    int const X_UNSORT[N] = {
        85, 91, 41, -75, -29, -71, 9, -89, -96, -48,
        29, 84, -77, 79, -49, 36, 30, 28, 13, 77
    };
    int const X_MAXSORT[N] = {
        91, 85, 84, 79, 77, 41, 36, 30, 29, 28,
        13, 9, -29, -48, -49, -71, -75, -77, -89, -96
    };
    int const X_MINSORT[N] = {
        -96, -89, -77, -75, -71, -49, -48, -29, 9, 13,
        28, 29, 30, 36, 41, 77, 79, 84, 85, 91
    };

    
    template <class Heap, class It, class TopOrder>
    void testPush(Heap & h, It first, It last,
                  TopOrder const & isOver)
    {
        if(first == last)
            return;

        typename Heap::value_t topSoFar = (h.empty() ? *first : h.top());
        for(; first != last; ++first)
        {
            if(isOver(*first, topSoFar))
                topSoFar = *first;

            h.push(*first);
            BOOST_CHECK_EQUAL(h.top(), topSoFar);
        }
    }

    template <class Heap, class It>
    void testFixedPush(Heap & h, size_t k, It first, It last)
    {
        if(first == last)
            return;

        for(; first != last; ++first)
        {
            if(h.size() < k)
                h.push(*first);
            else if(h.underTop(*first))
                h.replace(*first);
        }
    }

    template <class Heap, class It>
    void testPop(Heap & h, It first, It last)
    {
        for(; first != last; ++first)
        {
            BOOST_REQUIRE(!h.empty());
            BOOST_CHECK_EQUAL(h.top(), *first);
            h.pop();
        }
    }
}

BOOST_AUTO_TEST_CASE(maxheap_basics)
{
    MaxHeap<int> h;

    BOOST_CHECK(h.empty());
    BOOST_CHECK_EQUAL(h.size(), 0u);

    testPush(h, X_UNSORT, X_UNSORT + N, std::greater<int>());

    BOOST_CHECK(!h.empty());
    BOOST_CHECK_EQUAL(h.size(), N);

    testPop(h, X_MAXSORT, X_MAXSORT + N);

    BOOST_CHECK(h.empty());
    BOOST_CHECK_EQUAL(h.size(), 0u);
}

BOOST_AUTO_TEST_CASE(minheap_basics)
{
    MinHeap<int> h;

    BOOST_CHECK(h.empty());
    BOOST_CHECK_EQUAL(h.size(), 0u);

    testPush(h, X_UNSORT, X_UNSORT + N, std::less<int>());

    BOOST_CHECK(!h.empty());
    BOOST_CHECK_EQUAL(h.size(), N);

    testPop(h, X_MINSORT, X_MINSORT + N);

    BOOST_CHECK(h.empty());
    BOOST_CHECK_EQUAL(h.size(), 0u);
}

BOOST_AUTO_TEST_CASE(maxheap_fixed)
{
    MaxHeap<int> h;
    h.reserve(K);

    BOOST_CHECK(h.empty());
    BOOST_CHECK_EQUAL(h.size(), 0u);

    testFixedPush(h, K, X_UNSORT, X_UNSORT + N);

    BOOST_CHECK(!h.empty());
    BOOST_CHECK_EQUAL(h.size(), K);

    testPop(h, X_MAXSORT + K, X_MAXSORT + N);

    BOOST_CHECK(h.empty());
    BOOST_CHECK_EQUAL(h.size(), 0u);
}

BOOST_AUTO_TEST_CASE(minheap_fixed)
{
    MinHeap<int> h;
    h.reserve(K);

    BOOST_CHECK(h.empty());
    BOOST_CHECK_EQUAL(h.size(), 0u);

    testFixedPush(h, K, X_UNSORT, X_UNSORT + N);

    BOOST_CHECK(!h.empty());
    BOOST_CHECK_EQUAL(h.size(), K);

    testPop(h, X_MINSORT + K, X_MINSORT + N);

    BOOST_CHECK(h.empty());
    BOOST_CHECK_EQUAL(h.size(), 0u);
}
