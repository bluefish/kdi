//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
// Created 2008-02-16
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

#include <warp/bloom_filter.h>
#include <warp/hashtable.h>
#include <unittest/main.h>

using namespace warp;
using namespace ex;
using namespace std;

namespace
{
    uint32_t const SEEDS[] = {
        0x59ccc636,0x02b6a022,0x0d42d718,0xd0c37dbf,0xec894842,0x129203e9,
        0x202367ce,0xe10be0df,0xcf42a07e,0x1e713564,0x632c99f8,0xa1fe97ba,
        0xedd08f5c,0x0b14e2cf,0xd79322b1,0x8027be72,0xe2149208,0x0b6cecd0,
        0x7d11e363,0xae774908,0x15c35254,0x3e1e0877,0x619a9898,0x0324c63e,
        0xfb30fa2c,0x9ddc1851,0x65e969a9,0xebe6793f,0xab69ffac,0x14711612
    };

    inline uint64_t genItem()
    {
        return ((uint64_t(rand() & 0xffffffff) << 32) |
                uint64_t(rand() & 0xffffffff));
    }

    // Make Bloom filter with M bits, using K hashes.  Insert N random
    // items into the filter, then test and return the false positive
    // rate.  This will keep a hash set of all the items inserted, so
    // don't make N too large.
    double measureFalsePositives(size_t m, size_t n, size_t k)
    {
        srand(0);

        BloomFilter bloom(m, vector<uint32_t>(SEEDS, SEEDS+k));
        HashTable<uint64_t> items;

        // Insert N unique items
        items.reserve(n);
        while(items.size() < n)
        {
            uint64_t x = genItem();
            if(!items.contains(x))
            {
                items.insert(x);
                bloom.insert(binary_data(x));
            }
        }

        // Test for false positives
        size_t nFalse = 0;
        size_t nTrials = 0;
        while(nTrials < 50000)
        {
            uint64_t x = genItem();
            if(items.contains(x))
                continue;

            ++nTrials;
            if(bloom.contains(binary_data(x)))
                ++nFalse;
        }

        return double(nFalse) / nTrials;
    }
}

BOOST_AUTO_UNIT_TEST(basic)
{
    BloomFilter bloom(129, 3);

    // Should be empty
    BOOST_CHECK(!bloom.contains("hello"));
    BOOST_CHECK(!bloom.contains("goodbye"));

    // Insert something -- should contain it
    bloom.insert("hello");
    BOOST_CHECK(bloom.contains("hello"));
    
    // Insert something else -- should contain both
    bloom.insert("goodbye");
    BOOST_CHECK(bloom.contains("hello"));
    BOOST_CHECK(bloom.contains("goodbye"));

    // Serialize filter
    vector<char> buf;
    bloom.serialize(buf);
    BOOST_CHECK(!buf.empty());
    BOOST_CHECK(bloom.contains("hello"));
    BOOST_CHECK(bloom.contains("goodbye"));

    // Clear filter -- should be empty
    bloom.clear();
    BOOST_CHECK(!bloom.contains("hello"));
    BOOST_CHECK(!bloom.contains("goodbye"));

    // Restore filter -- should contain old data
    bloom.load(buf);
    BOOST_CHECK(bloom.contains("hello"));
    BOOST_CHECK(bloom.contains("goodbye"));

    // Try to load bogus data
    BOOST_CHECK_THROW(bloom.load(""), ValueError);
    BOOST_CHECK_THROW(bloom.load("this is unlikely to work"), ValueError);

    // Make a filter using the serialized data constructor
    BOOST_CHECK(BloomFilter(buf).contains("hello"));
    BOOST_CHECK(BloomFilter(buf).contains("goodbye"));
    BOOST_CHECK_THROW(BloomFilter(""), ValueError);
    BOOST_CHECK_THROW(BloomFilter("this is unlikely to work"), ValueError);
}

BOOST_AUTO_UNIT_TEST(filter_union)
{
    // Make two filters with identical parameters
    BloomFilter a(129, vector<uint32_t>(SEEDS, SEEDS+3));
    BloomFilter b(a.getBitCount(), a.getHashSeeds());

    // Add some stuff to each
    a.insert("banjo");
    a.insert("guitar");
    a.insert("mandolin");

    b.insert("pianoforte");
    b.insert("harpsichord");
    b.insert("clavichord");

    // Make sure each contains what they should
    BOOST_CHECK(a.contains("banjo"));
    BOOST_CHECK(a.contains("guitar"));
    BOOST_CHECK(a.contains("mandolin"));
    BOOST_CHECK(b.contains("pianoforte"));
    BOOST_CHECK(b.contains("harpsichord"));
    BOOST_CHECK(b.contains("clavichord"));

    // Check to see that they don't contain other things -- we could
    // get false positives, but the chances are low.
    BOOST_CHECK(!b.contains("banjo"));
    BOOST_CHECK(!b.contains("guitar"));
    BOOST_CHECK(!b.contains("mandolin"));
    BOOST_CHECK(!a.contains("pianoforte"));
    BOOST_CHECK(!a.contains("harpsichord"));
    BOOST_CHECK(!a.contains("clavichord"));

    // Insert all the items from B into A
    a.insert(b);
    
    // Make sure A contains everything from A and B
    BOOST_CHECK(a.contains("banjo"));
    BOOST_CHECK(a.contains("guitar"));
    BOOST_CHECK(a.contains("mandolin"));
    BOOST_CHECK(a.contains("pianoforte"));
    BOOST_CHECK(a.contains("harpsichord"));
    BOOST_CHECK(a.contains("clavichord"));

    // Check that we can't insert filters with different parameters
    BloomFilter c(101, 4);
    c.insert("yogurt");
    BOOST_CHECK_THROW(a.insert(c), ValueError);
}

BOOST_AUTO_UNIT_TEST(error_rate)
{
    // Expected error rate chart:
    //   http://pages.cs.wisc.edu/~cao/papers/summary-cache/node8.html

    // Check that each filter implementation has a false positive rate
    // better than 30% over the expected rate.

    // M/N=3
    BOOST_CHECK(measureFalsePositives(60000, 20000, 1) < 0.283 * 1.30);
    BOOST_CHECK(measureFalsePositives(60000, 20000, 2) < 0.237 * 1.30);
    BOOST_CHECK(measureFalsePositives(60000, 20000, 3) < 0.253 * 1.30);

    // M/N=5
    BOOST_CHECK(measureFalsePositives(100000, 20000, 1) < 0.181 * 1.30);
    BOOST_CHECK(measureFalsePositives(100000, 20000, 2) < 0.109 * 1.30);
    BOOST_CHECK(measureFalsePositives(100000, 20000, 3) < 0.092 * 1.30);
    BOOST_CHECK(measureFalsePositives(100000, 20000, 4) < 0.092 * 1.30);
    BOOST_CHECK(measureFalsePositives(100000, 20000, 5) < 0.101 * 1.30);

    // M/N=10
    BOOST_CHECK(measureFalsePositives(200000, 20000, 1) < 0.09520 * 1.30);
    BOOST_CHECK(measureFalsePositives(200000, 20000, 2) < 0.03290 * 1.30);
    BOOST_CHECK(measureFalsePositives(200000, 20000, 3) < 0.01740 * 1.30);
    BOOST_CHECK(measureFalsePositives(200000, 20000, 4) < 0.01180 * 1.30);
    BOOST_CHECK(measureFalsePositives(200000, 20000, 5) < 0.00943 * 1.30);
    BOOST_CHECK(measureFalsePositives(200000, 20000, 6) < 0.00844 * 1.30);
    BOOST_CHECK(measureFalsePositives(200000, 20000, 7) < 0.00819 * 1.30);
    BOOST_CHECK(measureFalsePositives(200000, 20000, 8) < 0.00846 * 1.30);

    // M/N=15
    BOOST_CHECK(measureFalsePositives(300000, 20000,  1) < 0.064500 * 1.30);
    BOOST_CHECK(measureFalsePositives(300000, 20000,  2) < 0.015600 * 1.30);
    BOOST_CHECK(measureFalsePositives(300000, 20000,  3) < 0.005960 * 1.30);
    BOOST_CHECK(measureFalsePositives(300000, 20000,  4) < 0.003000 * 1.30);
    BOOST_CHECK(measureFalsePositives(300000, 20000,  5) < 0.001830 * 1.30);
    BOOST_CHECK(measureFalsePositives(300000, 20000,  6) < 0.001280 * 1.30);
    BOOST_CHECK(measureFalsePositives(300000, 20000,  7) < 0.001000 * 1.30);
    BOOST_CHECK(measureFalsePositives(300000, 20000,  8) < 0.000852 * 1.30);
    BOOST_CHECK(measureFalsePositives(300000, 20000,  9) < 0.000775 * 1.30);
    BOOST_CHECK(measureFalsePositives(300000, 20000, 10) < 0.000744 * 1.30);
    BOOST_CHECK(measureFalsePositives(300000, 20000, 11) < 0.000747 * 1.30);
    BOOST_CHECK(measureFalsePositives(300000, 20000, 12) < 0.000778 * 1.30);
}

BOOST_AUTO_UNIT_TEST(inplace_bloom_contains)
{
    // Test the static contains() function

    // First build a BloomFilter
    BloomFilter f(129, 3);
    f.insert("banjo");
    f.insert("guitar");
    f.insert("mandolin");
    f.insert("pianoforte");
    f.insert("harpsichord");

    // Serialize it
    vector<char> s;
    f.serialize(s);

    // Make sure they both contain the same things
    BOOST_CHECK_EQUAL(f.contains("banjo")       , BloomFilter::contains(s, "banjo"));
    BOOST_CHECK_EQUAL(f.contains("guitar")      , BloomFilter::contains(s, "guitar"));
    BOOST_CHECK_EQUAL(f.contains("mandolin")    , BloomFilter::contains(s, "mandolin"));
    BOOST_CHECK_EQUAL(f.contains("pianoforte")  , BloomFilter::contains(s, "pianoforte"));
    BOOST_CHECK_EQUAL(f.contains("harpsichord") , BloomFilter::contains(s, "harpsichord"));

    BOOST_CHECK_EQUAL(f.contains("blender")     , BloomFilter::contains(s, "blender"));
    BOOST_CHECK_EQUAL(f.contains("sink")        , BloomFilter::contains(s, "sink"));
    BOOST_CHECK_EQUAL(f.contains("spatula")     , BloomFilter::contains(s, "spatula"));
    BOOST_CHECK_EQUAL(f.contains("toaster")     , BloomFilter::contains(s, "toaster"));
    BOOST_CHECK_EQUAL(f.contains("oven")        , BloomFilter::contains(s, "oven"));
}
