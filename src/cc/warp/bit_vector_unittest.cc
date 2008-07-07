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

#include <warp/bit_vector.h>
#include <unittest/main.h>
#include <vector>
extern "C" {
#include <string.h>
}

using namespace warp;

namespace {
    void basicTest(size_t const K)
    {
        BitVector v(K);
    
        // Should be empty
        BOOST_CHECK_EQUAL(v.size(), K);
        BOOST_CHECK_EQUAL(v.population(), 0u);
        for(size_t i = 0; i < K; ++i)
            BOOST_CHECK(!v.test(i));
    
        // Set even bits
        for(size_t i = 0; i < K; i += 2)
            v.set(i);

        // Check bits - even should be set
        BOOST_CHECK_EQUAL(v.population(), (K+1)/2);
        for(size_t i = 0; i < K; ++i)
            BOOST_CHECK_EQUAL(v.test(i), (i%2) == 0);

        // Flip all bits
        for(size_t i = 0; i < K; ++i)
            v.flip(i);

        // Check bits - odd should be set
        BOOST_CHECK_EQUAL(v.population(), K/2);
        for(size_t i = 0; i < K; ++i)
            BOOST_CHECK_EQUAL(v.test(i), (i%2) == 1);

        // Save a copy -- add an extra byte so we don't have to worry
        // about buf having size 0
        std::vector<uint8_t> buf(v.rawSize()+1);
        memcpy(&buf[0], v.raw(), v.rawSize());

        // Clear all bits
        v.clear();

        // Should be empty
        BOOST_CHECK_EQUAL(v.population(), 0u);
        for(size_t i = 0; i < K; ++i)
            BOOST_CHECK(!v.test(i));

        // Reload buffer
        v.load(K, &buf[0]);

        // Check bits - odd should be set
        BOOST_CHECK_EQUAL(v.size(), K);
        BOOST_CHECK_EQUAL(v.population(), K/2);
        for(size_t i = 0; i < K; ++i)
            BOOST_CHECK_EQUAL(v.test(i), (i%2) == 1);

        // Set all bits in buffer and load again.  This will include
        // set bits in the last byte of the buffer that aren't in the
        // K bits of the vector
        std::fill(buf.begin(), buf.end(), 0xffu);
        v.load(K, &buf[0]);

        // Check bits - all should be set
        BOOST_CHECK_EQUAL(v.size(), K);
        BOOST_CHECK_EQUAL(v.population(), K);
        for(size_t i = 0; i < K; ++i)
            BOOST_CHECK(v.test(i));

        // Make another bit vector using copy constructor
        BitVector v2(v);

        // Check bits - all should be set
        BOOST_CHECK_EQUAL(v2.size(), K);
        BOOST_CHECK_EQUAL(v2.population(), K);
        for(size_t i = 0; i < K; ++i)
            BOOST_CHECK(v2.test(i));

        // Clear bit positions that aren't a multiple of 5 in V and
        // bit positions that aren't a multiple of 7 in V2
        for(size_t i = 0; i < K; ++i)
        {
            if(i % 5)
                v.clear(i);
            if(i % 7)
                v2.clear(i);
        }

        // Check bits - 1/5 for V, and 1/7 for V2
        for(size_t i = 0; i < K; ++i)
        {
            BOOST_CHECK_EQUAL(v.test(i),  (i % 5) == 0);
            BOOST_CHECK_EQUAL(v2.test(i), (i % 7) == 0);
        }

        // Get bitwise-OR of V and V2
        BitVector v3 = v | v2;
        
        // Check bits - 1/(5 or 7) for V3
        for(size_t i = 0; i < K; ++i)
            BOOST_CHECK_EQUAL(v3.test(i), (i % 5) == 0 || (i % 7) == 0);

        // Get bitwise-AND of V and V2
        v3 = v & v2;

        // Check bits - 1/(5 and 7) for V3
        for(size_t i = 0; i < K; ++i)
            BOOST_CHECK_EQUAL(v3.test(i), (i % 35) == 0);

        // Get bitwise-XOR of V and V2
        v3 = v ^ v2;

        // Check bits - 1/(5 xor 7) for V3
        for(size_t i = 0; i < K; ++i)
            BOOST_CHECK_EQUAL(v3.test(i), ((i % 5) == 0 || (i % 7) == 0)
                              && (i % 35) != 0);
    }
}


BOOST_AUTO_UNIT_TEST(basic_0) { basicTest(0); }
BOOST_AUTO_UNIT_TEST(basic_1) { basicTest(1); }
BOOST_AUTO_UNIT_TEST(basic_2) { basicTest(2); }
BOOST_AUTO_UNIT_TEST(basic_3) { basicTest(3); }
BOOST_AUTO_UNIT_TEST(basic_4) { basicTest(4); }
BOOST_AUTO_UNIT_TEST(basic_5) { basicTest(5); }
BOOST_AUTO_UNIT_TEST(basic_6) { basicTest(6); }
BOOST_AUTO_UNIT_TEST(basic_7) { basicTest(7); }
BOOST_AUTO_UNIT_TEST(basic_8) { basicTest(8); }

BOOST_AUTO_UNIT_TEST(basic_1024) { basicTest(1024); }
BOOST_AUTO_UNIT_TEST(basic_1025) { basicTest(1025); }
BOOST_AUTO_UNIT_TEST(basic_1026) { basicTest(1026); }
BOOST_AUTO_UNIT_TEST(basic_1027) { basicTest(1027); }
BOOST_AUTO_UNIT_TEST(basic_1028) { basicTest(1028); }
BOOST_AUTO_UNIT_TEST(basic_1029) { basicTest(1029); }
BOOST_AUTO_UNIT_TEST(basic_1030) { basicTest(1030); }
BOOST_AUTO_UNIT_TEST(basic_1031) { basicTest(1031); }
BOOST_AUTO_UNIT_TEST(basic_1032) { basicTest(1032); }
