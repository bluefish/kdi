//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-07-27
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

#include "rankcollector.h"
#include "unittest/main.h"
#include "unittest/predicates.h"

using namespace warp;
using unittest::equal_unordered_collections;

namespace
{
    template <class Collector, class It>
    void testCollector(Collector & collector, It begin, It end)
    {
        typedef typename std::iterator_traits<It>::value_type value_t;
        typedef std::vector<value_t> vec_t;
        typedef typename Collector::value_compare_t cmp_t;

        cmp_t const & lt = collector.valueCompare();
        vec_t vec;

        // Put each item into the collector and a vector
        for(; begin != end; ++begin)
        {
            collector.insert(*begin);
            vec.push_back(*begin);
        }

        // Sort vector and erase the tail to get top-K
        std::sort(vec.begin(), vec.end(), lt);
        vec.erase(vec.begin() + collector.size(), vec.end());


        // Check to see if rank collector top-K matches sorted top-K
        BOOST_CHECK(equal_unordered_collections(
                        collector.begin(), collector.end(),
                        vec.begin(), vec.end()));
    }
}

//----------------------------------------------------------------------------
// PartitionRankCollector tests
//----------------------------------------------------------------------------
BOOST_AUTO_UNIT_TEST(basic_partition)
{
    size_t const K = 5;
    size_t const N = 10;
    int const X[N] = {
        27, -98, -4, -98, -52, -20, 52, -65, 34, 48,
    };

    PartitionRankCollector<int> c(K);

    BOOST_CHECK_EQUAL(c.empty(), true);
    BOOST_CHECK_EQUAL(c.size(), 0u);

    c.insert(X[0]);

    BOOST_CHECK_EQUAL(c.empty(), false);
    BOOST_CHECK_EQUAL(c.size(), 1u);
    BOOST_CHECK(equal_unordered_collections(c.begin(), c.end(), X, X + 1));

    c.insert(X[1]);
    c.insert(X[2]);
    c.insert(X[3]);
    c.insert(X[4]);
    c.insert(X[5]);
    c.insert(X[6]);

    int const TOP1[K] = { -98, -4, -98, -52, -20 };

    BOOST_CHECK_EQUAL(c.size(), K);
    BOOST_CHECK(equal_unordered_collections(c.begin(), c.end(), TOP1, TOP1 + K));

    c.insert(X[7]);

    int const TOP2[K] = { -98, -65, -98, -52, -20 };

    BOOST_CHECK(equal_unordered_collections(c.begin(), c.end(), TOP2, TOP2 + K));
    
    c.clear();

    BOOST_CHECK_EQUAL(c.empty(), true);
    BOOST_CHECK_EQUAL(c.size(), 0u);

    c.insert(X[8]);
    c.insert(X[9]);

    int const TOP3[2] = { 34, 48 };

    BOOST_CHECK(equal_unordered_collections(c.begin(), c.end(), TOP3, TOP3 + 2));
}

BOOST_AUTO_UNIT_TEST(unordered_partition)
{
    size_t const N = 50;
    int const X[N] = {
        71, -3, 98, -7, 63, -4, -1, 31, 79, 92,
        -96, 17, -26, 28, -20, 20, -36, 31, -71, 91,
        27, -98, -4, -98, -52, -20, 52, -65, 34, 48,
        76, 80, 17, 94, -61, 41, 75, -89, -22, -14,
        -77, -32, -46, 15, 12, -42, 91, 71, -6, -52
    };

    PartitionRankCollector<int> c(10);
    testCollector(c, X, X + N);
}

BOOST_AUTO_UNIT_TEST(underfill_partition)
{
    size_t const N = 50;
    int const X[N] = {
        71, -3, 98, -7, 63, -4, -1, 31, 79, 92,
        -96, 17, -26, 28, -20, 20, -36, 31, -71, 91,
        27, -98, -4, -98, -52, -20, 52, -65, 34, 48,
        76, 80, 17, 94, -61, 41, 75, -89, -22, -14,
        -77, -32, -46, 15, 12, -42, 91, 71, -6, -52
    };

    PartitionRankCollector<int> c(100);
    testCollector(c, X, X + N);
}

BOOST_AUTO_UNIT_TEST(ascending_partition)
{
    size_t const N = 50;
    int const X[N] = {
        -94, -94, -87, -78, -78, -71, -70, -58, -57, -50,
        -46, -38, -34, -32, -28, -24, -19, -18, -15, -12,
        -11, -10, -8, -5, 3, 9, 11, 11, 13, 15,
        23, 37, 43, 45, 45, 48, 52, 53, 54, 64,
        65, 69, 71, 77, 83, 85, 85, 85, 88, 93
    };

    PartitionRankCollector<int> c(10);
    testCollector(c, X, X + N);
}

BOOST_AUTO_UNIT_TEST(descending_partition)
{
    size_t const N = 50;
    int const X[N] = {
        93, 88, 85, 85, 85, 83, 77, 71, 69, 65,
        64, 54, 53, 52, 48, 45, 45, 43, 37, 23,
        15, 13, 11, 11, 9, 3, -5, -8, -10, -11,
        -12, -15, -18, -19, -24, -28, -32, -34, -38, -46,
        -50, -57, -58, -70, -71, -78, -78, -87, -94, -94
    };

    PartitionRankCollector<int> c(10);
    testCollector(c, X, X + N);
}

BOOST_AUTO_UNIT_TEST(partition_reject)
{
    PartitionRankCollector<int> c(5);

    // We should be able to insert up to 5 things without any
    // rejections
    BOOST_CHECK(!c.wouldReject(0));
    BOOST_CHECK(c.insert(0));

    BOOST_CHECK(!c.wouldReject(1));
    BOOST_CHECK(c.insert(1));

    BOOST_CHECK(!c.wouldReject(2));
    BOOST_CHECK(c.insert(2));

    BOOST_CHECK(!c.wouldReject(3));
    BOOST_CHECK(c.insert(3));

    BOOST_CHECK(!c.wouldReject(4));
    BOOST_CHECK(c.insert(4));

    // This should get rejected because it is lower or equal rank
    // compared to everything in the collection
    BOOST_CHECK(c.wouldReject(5));
    BOOST_CHECK(!c.insert(5));

    // This should get rejected because it is lower or equal rank
    // compared to everything in the collection
    BOOST_CHECK(c.wouldReject(4));
    BOOST_CHECK(!c.insert(4));

    // This should displace the 4, and shouldn't be rejected
    BOOST_CHECK(!c.wouldReject(3));
    BOOST_CHECK(c.insert(3));
}



//----------------------------------------------------------------------------
// HeapRankCollector tests
//----------------------------------------------------------------------------
BOOST_AUTO_UNIT_TEST(basic_heap)
{
    size_t const K = 5;
    size_t const N = 10;
    int const X[N] = {
        27, -98, -4, -98, -52, -20, 52, -65, 34, 48,
    };

    HeapRankCollector<int> c(K);

    BOOST_CHECK_EQUAL(c.empty(), true);
    BOOST_CHECK_EQUAL(c.size(), 0u);

    c.insert(X[0]);

    BOOST_CHECK_EQUAL(c.empty(), false);
    BOOST_CHECK_EQUAL(c.size(), 1u);
    BOOST_CHECK(equal_unordered_collections(c.begin(), c.end(), X, X + 1));

    c.insert(X[1]);
    c.insert(X[2]);
    c.insert(X[3]);
    c.insert(X[4]);
    c.insert(X[5]);
    c.insert(X[6]);

    int const TOP1[K] = { -98, -4, -98, -52, -20 };

    BOOST_CHECK_EQUAL(c.size(), K);
    BOOST_CHECK(equal_unordered_collections(c.begin(), c.end(), TOP1, TOP1 + K));

    c.insert(X[7]);

    int const TOP2[K] = { -98, -65, -98, -52, -20 };

    BOOST_CHECK(equal_unordered_collections(c.begin(), c.end(), TOP2, TOP2 + K));
    
    c.clear();

    BOOST_CHECK_EQUAL(c.empty(), true);
    BOOST_CHECK_EQUAL(c.size(), 0u);

    c.insert(X[8]);
    c.insert(X[9]);

    int const TOP3[2] = { 34, 48 };

    BOOST_CHECK(equal_unordered_collections(c.begin(), c.end(), TOP3, TOP3 + 2));
}

BOOST_AUTO_UNIT_TEST(unordered_heap)
{
    size_t const N = 50;
    int const X[N] = {
        71, -3, 98, -7, 63, -4, -1, 31, 79, 92,
        -96, 17, -26, 28, -20, 20, -36, 31, -71, 91,
        27, -98, -4, -98, -52, -20, 52, -65, 34, 48,
        76, 80, 17, 94, -61, 41, 75, -89, -22, -14,
        -77, -32, -46, 15, 12, -42, 91, 71, -6, -52
    };

    HeapRankCollector<int> c(10);
    testCollector(c, X, X + N);
}

BOOST_AUTO_UNIT_TEST(underfill_heap)
{
    size_t const N = 50;
    int const X[N] = {
        71, -3, 98, -7, 63, -4, -1, 31, 79, 92,
        -96, 17, -26, 28, -20, 20, -36, 31, -71, 91,
        27, -98, -4, -98, -52, -20, 52, -65, 34, 48,
        76, 80, 17, 94, -61, 41, 75, -89, -22, -14,
        -77, -32, -46, 15, 12, -42, 91, 71, -6, -52
    };

    HeapRankCollector<int> c(100);
    testCollector(c, X, X + N);
}

BOOST_AUTO_UNIT_TEST(ascending_heap)
{
    size_t const N = 50;
    int const X[N] = {
        -94, -94, -87, -78, -78, -71, -70, -58, -57, -50,
        -46, -38, -34, -32, -28, -24, -19, -18, -15, -12,
        -11, -10, -8, -5, 3, 9, 11, 11, 13, 15,
        23, 37, 43, 45, 45, 48, 52, 53, 54, 64,
        65, 69, 71, 77, 83, 85, 85, 85, 88, 93
    };

    HeapRankCollector<int> c(10);
    testCollector(c, X, X + N);
}

BOOST_AUTO_UNIT_TEST(descending_heap)
{
    size_t const N = 50;
    int const X[N] = {
        93, 88, 85, 85, 85, 83, 77, 71, 69, 65,
        64, 54, 53, 52, 48, 45, 45, 43, 37, 23,
        15, 13, 11, 11, 9, 3, -5, -8, -10, -11,
        -12, -15, -18, -19, -24, -28, -32, -34, -38, -46,
        -50, -57, -58, -70, -71, -78, -78, -87, -94, -94
    };

    HeapRankCollector<int> c(10);
    testCollector(c, X, X + N);
}
