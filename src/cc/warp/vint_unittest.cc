//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-03-06
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

#include <warp/vint.h>
#include <unittest/main.h>

using namespace warp::vint;

namespace
{
    template <class T>
    struct VIntTest
    {
        T inputNumber;
        size_t encodedSize;
    };

    struct VQuadTest
    {
        uint32_t inputQuad[4];
        size_t encodedSize;
    };
}

//----------------------------------------------------------------------------
// unsigned vint64
//----------------------------------------------------------------------------
BOOST_AUTO_UNIT_TEST(test_v64)
{
    VIntTest<uint64_t> const tests[] = {
        { 0u, 1 },                      // 0
        { 1u, 1 },                      // 1
        { 2u, 1 },                      // 2^1
        { 10u, 1 },                     // 10^1
        { 100u, 1 },                    // 10^2
        { 127u, 1 },                    // 2^7-1
        { 128u, 2 },                    // 2^7
        { 1000u, 2 },                   // 10^3
        { 10000u, 2 },                  // 10^4
        { 16383u, 2 },                  // 2^14-1
        { 16384u, 3 },                  // 2^14
        { 100000u, 3 },                 // 10^5
        { 1000000u, 3 },                // 10^6
        { 2097151u, 3 },                // 2^21-1
        { 2097152u, 4 },                // 2^21
        { 10000000u, 4 },               // 10^7
        { 100000000u, 4 },              // 10^8
        { 268435455u, 4 },              // 2^28-1
        { 268435456u, 5 },              // 2^28
        { 1000000000u, 5 },             // 10^9
        { 4294967295u, 5 },             // 2^32-1
        { 4294967296u, 5 },             // 2^32
        { 10000000000u, 5 },            // 10^10
        { 34359738367u, 5 },            // 2^35-1
        { 34359738368u, 6 },            // 2^35
        { 100000000000u, 6 },           // 10^11
        { 1000000000000u, 6 },          // 10^12
        { 4398046511103u, 6 },          // 2^42-1
        { 4398046511104u, 7 },          // 2^42
        { 10000000000000u, 7 },         // 10^13
        { 100000000000000u, 7 },        // 10^14
        { 562949953421311u, 7 },        // 2^49-1
        { 562949953421312u, 8 },        // 2^49
        { 1000000000000000u, 8 },       // 10^15
        { 10000000000000000u, 8 },      // 10^16
        { 72057594037927935u, 8 },      // 2^56-1
        { 72057594037927936u, 9 },      // 2^56
        { 100000000000000000u, 9 },     // 10^17
        { 1000000000000000000u, 9 },    // 10^18
        { 10000000000000000000u, 9 },   // 10^19
        { 18446744073709551615u, 9 },   // 2^64-1
    };
    size_t const N = sizeof(tests) / sizeof(*tests);

    char buf[MAX_VINT64_SIZE];
    uint64_t outputNumber;
    uint64_t outputNumber2;

    for(size_t i = 0; i < N; ++i)
    {
        BOOST_CHECK_EQUAL(getEncodedLength(tests[i].inputNumber), tests[i].encodedSize);
        BOOST_CHECK_EQUAL(encode(tests[i].inputNumber, buf), tests[i].encodedSize);
        BOOST_CHECK_EQUAL(decode(buf, outputNumber), tests[i].encodedSize);
        BOOST_CHECK_EQUAL(tests[i].inputNumber, outputNumber);
        BOOST_CHECK_EQUAL(decodeSafe(buf, tests[i].encodedSize, outputNumber2), tests[i].encodedSize);
        BOOST_CHECK_EQUAL(tests[i].inputNumber, outputNumber2);
    }
}

//----------------------------------------------------------------------------
// unsigned vint32
//----------------------------------------------------------------------------
BOOST_AUTO_UNIT_TEST(test_v32)
{
    VIntTest<uint32_t> const tests[] = {
        { 0u, 1 },                      // 0
        { 1u, 1 },                      // 1
        { 2u, 1 },                      // 2^1
        { 10u, 1 },                     // 10^1
        { 100u, 1 },                    // 10^2
        { 127u, 1 },                    // 2^7-1
        { 128u, 2 },                    // 2^7
        { 1000u, 2 },                   // 10^3
        { 10000u, 2 },                  // 10^4
        { 16383u, 2 },                  // 2^14-1
        { 16384u, 3 },                  // 2^14
        { 100000u, 3 },                 // 10^5
        { 1000000u, 3 },                // 10^6
        { 2097151u, 3 },                // 2^21-1
        { 2097152u, 4 },                // 2^21
        { 10000000u, 4 },               // 10^7
        { 100000000u, 4 },              // 10^8
        { 268435455u, 4 },              // 2^28-1
        { 268435456u, 5 },              // 2^28
        { 1000000000u, 5 },             // 10^9
        { 4294967295u, 5 },             // 2^32-1
    };
    size_t const N = sizeof(tests) / sizeof(*tests);

    char buf[MAX_VINT32_SIZE];
    uint32_t outputNumber;
    uint32_t outputNumber2;

    for(size_t i = 0; i < N; ++i)
    {
        BOOST_CHECK_EQUAL(getEncodedLength(tests[i].inputNumber), tests[i].encodedSize);
        BOOST_CHECK_EQUAL(encode(tests[i].inputNumber, buf), tests[i].encodedSize);
        BOOST_CHECK_EQUAL(decode(buf, outputNumber), tests[i].encodedSize);
        BOOST_CHECK_EQUAL(tests[i].inputNumber, outputNumber);
        BOOST_CHECK_EQUAL(decodeSafe(buf, tests[i].encodedSize, outputNumber2), tests[i].encodedSize);
        BOOST_CHECK_EQUAL(tests[i].inputNumber, outputNumber2);
    }
}

//----------------------------------------------------------------------------
// signed vint64
//----------------------------------------------------------------------------
BOOST_AUTO_UNIT_TEST(test_v64_signed)
{
    VIntTest<int64_t> const tests[] = {
        {  0, 1 },                         //  0
        {  1, 1 },                         //  1
        { -1, 1 },                         // -1
           
        {  64, 2 },                        //  2^6
        {  8192, 3 },                      //  2^13
        {  1048576, 4 },                   //  2^20
        {  134217728, 5 },                 //  2^27
        {  17179869184, 6 },               //  2^34
        {  2199023255552, 7 },             //  2^41
        {  281474976710656, 8 },           //  2^48
        {  36028797018963968, 9 },         //  2^55
        {  4611686018427387904, 9 },       //  2^62
           
        {  63, 1 },                        //  2^6-1
        {  8191, 2 },                      //  2^13-1
        {  1048575, 3 },                   //  2^20-1
        {  134217727, 4 },                 //  2^27-1
        {  17179869183, 5 },               //  2^34-1
        {  2199023255551, 6 },             //  2^41-1
        {  281474976710655, 7 },           //  2^48-1
        {  36028797018963967, 8 },         //  2^55-1
        {  4611686018427387903, 9 },       //  2^62-1

        { -64, 1 },                        // -2^6
        { -8192, 2 },                      // -2^13
        { -1048576, 3 },                   // -2^20
        { -134217728, 4 },                 // -2^27
        { -17179869184, 5 },               // -2^34
        { -2199023255552, 6 },             // -2^41
        { -281474976710656, 7 },           // -2^48
        { -36028797018963968, 8 },         // -2^55
        { -4611686018427387904, 9 },       // -2^62

        {  9223372036854775807, 9 },       //  2^63-1
        { -9223372036854775807-1, 9 },     // -2^63

        {  1000, 2 },                      //  10^3
        {  1000000, 3 },                   //  10^6
        {  1000000000, 5 },                //  10^9
        {  1000000000000, 6 },             //  10^12
        {  1000000000000000, 8 },          //  10^15
        {  1000000000000000000, 9 },       //  10^18

        { -1000, 2 },                      // -10^3
        { -1000000, 3 },                   // -10^6
        { -1000000000, 5 },                // -10^9
        { -1000000000000, 6 },             // -10^12
        { -1000000000000000, 8 },          // -10^15
        { -1000000000000000000, 9 },       // -10^18
    };
    size_t const N = sizeof(tests) / sizeof(*tests);

    char buf[MAX_VINT64_SIZE];
    int64_t outputNumber;
    int64_t outputNumber2;

    for(size_t i = 0; i < N; ++i)
    {
        BOOST_CHECK_EQUAL(zigzagDecode(zigzagEncode(tests[i].inputNumber)), tests[i].inputNumber);
        BOOST_CHECK_EQUAL(getSignedEncodedLength(tests[i].inputNumber), tests[i].encodedSize);
        BOOST_CHECK_EQUAL(encodeSigned(tests[i].inputNumber, buf), tests[i].encodedSize);
        BOOST_CHECK_EQUAL(decodeSigned(buf, outputNumber), tests[i].encodedSize);
        BOOST_CHECK_EQUAL(tests[i].inputNumber, outputNumber);
        BOOST_CHECK_EQUAL(decodeSignedSafe(buf, tests[i].encodedSize, outputNumber2), tests[i].encodedSize);
        BOOST_CHECK_EQUAL(tests[i].inputNumber, outputNumber2);
    }
}

//----------------------------------------------------------------------------
// signed vint32
//----------------------------------------------------------------------------
BOOST_AUTO_UNIT_TEST(test_v32_signed)
{
    VIntTest<int32_t> const tests[] = {
        {  0, 1 },                         //  0
        {  1, 1 },                         //  1
        { -1, 1 },                         // -1
           
        {  64, 2 },                        //  2^6
        {  8192, 3 },                      //  2^13
        {  1048576, 4 },                   //  2^20
        {  134217728, 5 },                 //  2^27
           
        {  63, 1 },                        //  2^6-1
        {  8191, 2 },                      //  2^13-1
        {  1048575, 3 },                   //  2^20-1
        {  134217727, 4 },                 //  2^27-1

        { -64, 1 },                        // -2^6
        { -8192, 2 },                      // -2^13
        { -1048576, 3 },                   // -2^20
        { -134217728, 4 },                 // -2^27

        {  2147483647, 5 },                //  2^31-1
        { -2147483648, 5 },                // -2^31

        {  1000, 2 },                      //  10^3
        {  1000000, 3 },                   //  10^6
        {  1000000000, 5 },                //  10^9

        { -1000, 2 },                      // -10^3
        { -1000000, 3 },                   // -10^6
        { -1000000000, 5 },                // -10^9
    };
    size_t const N = sizeof(tests) / sizeof(*tests);

    char buf[MAX_VINT64_SIZE];
    int32_t outputNumber;
    int32_t outputNumber2;

    for(size_t i = 0; i < N; ++i)
    {
        BOOST_CHECK_EQUAL(zigzagDecode(zigzagEncode(tests[i].inputNumber)), tests[i].inputNumber);
        BOOST_CHECK_EQUAL(getSignedEncodedLength(tests[i].inputNumber), tests[i].encodedSize);
        BOOST_CHECK_EQUAL(encodeSigned(tests[i].inputNumber, buf), tests[i].encodedSize);
        BOOST_CHECK_EQUAL(decodeSigned(buf, outputNumber), tests[i].encodedSize);
        BOOST_CHECK_EQUAL(tests[i].inputNumber, outputNumber);
        BOOST_CHECK_EQUAL(decodeSignedSafe(buf, tests[i].encodedSize, outputNumber2), tests[i].encodedSize);
        BOOST_CHECK_EQUAL(tests[i].inputNumber, outputNumber2);
    }
}

//----------------------------------------------------------------------------
// unsigned quad vint32
//----------------------------------------------------------------------------
BOOST_AUTO_UNIT_TEST(test_vquad)
{
    VQuadTest const tests[] = {
        { {0u,0u,0u,0u}, 5 },
        { {1u,0u,1u,0u}, 5 },
        { {4u,3u,2u,1u}, 5 },
        { {1000u,10000u,100000u,1000000u}, 11 },
        { {1000000u,1000u,100000u,10000u}, 11 },
        { {0xffffffu,0xffffffffu,0xffffffffu,0xffffffffu}, 16 },
        { {0xffffffu,0xffffffffu,0xffffu,0xffu}, 11 },
        { {0x33u,0x44u,0x0216u,0x77u}, 6 },
        { {0xffffffffu,0xffffffffu,0xffffffffu,0xffffffffu}, 17 },
    };
    size_t const N = sizeof(tests) / sizeof(*tests);

    char buf[MAX_QUAD_SIZE];
    uint32_t outputQuad[4];
    uint32_t outputQuad2[4];

    for(size_t i = 0; i < N; ++i)
    {
        size_t totalByteLength = 0;
        for(size_t j = 0; j < 4; ++j)
            totalByteLength += getByteLength(tests[i].inputQuad[j]);
        BOOST_CHECK_EQUAL(totalByteLength + 1, tests[i].encodedSize);

        BOOST_CHECK_EQUAL(encodeQuad(tests[i].inputQuad, buf), tests[i].encodedSize);
        BOOST_CHECK_EQUAL(decodeQuad(buf, outputQuad), tests[i].encodedSize);
        BOOST_CHECK_EQUAL(decodeQuadSafe(buf, tests[i].encodedSize, outputQuad2), tests[i].encodedSize);

        for(size_t j = 0; j < 4; ++j)
        {
            BOOST_CHECK_EQUAL(tests[i].inputQuad[j], outputQuad[j]);
            BOOST_CHECK_EQUAL(tests[i].inputQuad[j], outputQuad2[j]);
        }
    }
}
