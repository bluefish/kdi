//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
// Created 2008-08-26
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

#include <unittest/main.h>
#include <unittest/predicates.h>
#include <warp/base64.h>
#include <warp/strutil.h>
#include <string>

using namespace warp;
using namespace std;
using namespace unittest;

namespace
{
    struct Checker
    {
        Base64Encoder enc;
        Base64Decoder dec;
        
        Checker() {}

        explicit Checker(strref_t alphabet) :
            enc(alphabet), dec(alphabet) {}

        predicate_result codecTest(strref_t plaintext, strref_t ciphertext)
        {
            string encoded;
            enc.encode(plaintext, encoded);

            string decoded;
            dec.decode(ciphertext, decoded);

            predicate_result r(true);
            if(encoded != ciphertext)
            {
                r = false;
                r.message() << "encoded != ciphertext: "
                            << reprString(encoded) << " != "
                            << reprString(ciphertext) << ".  ";
            }
            if(decoded != plaintext)
            {
                r = false;
                r.message() << "decoded != plaintext: "
                            << reprString(decoded) << " != "
                            << reprString(plaintext) << ".  ";
            }
            return r;
        }

        predicate_result orderTest(strref_t a, strref_t b)
        {
            string ae;
            enc.encode(a, ae);

            string be;
            enc.encode(b, be);

            predicate_result r(true);

            if((a < b) != (ae < be) || (b < a) != (be < ae))
            {
                r = false;
                r.message() << "encoding changed ordering: orig("
                            << reprString(a) << ", " << reprString(b)
                            << ") enc(" << reprString(ae) << ", "
                            << reprString(be) << ").";
            }
            return r;
        }
    };
}

BOOST_AUTO_UNIT_TEST(pem_test)
{
    Checker c;

    BOOST_CHECK(c.codecTest("", ""));
    BOOST_CHECK(c.codecTest("a", "YQ"));
    BOOST_CHECK(c.codecTest("at", "YXQ"));
    BOOST_CHECK(c.codecTest("ate", "YXRl"));
    BOOST_CHECK(c.codecTest(binary_data("\x00\x00\x00",3), "AAAA"));
    BOOST_CHECK(c.codecTest("\xff\xff\xff", "////"));
    BOOST_CHECK(c.codecTest("Hello there", "SGVsbG8gdGhlcmU"));
}

BOOST_AUTO_UNIT_TEST(sortable_test)
{
    Checker c(
        "+/0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz"
        );

    BOOST_CHECK(c.codecTest("", ""));
    BOOST_CHECK(c.codecTest("a", "ME"));
    BOOST_CHECK(c.codecTest("at", "MLE"));
    BOOST_CHECK(c.codecTest("ate", "MLFZ"));
    BOOST_CHECK(c.codecTest(binary_data("\x00\x00\x00",3), "++++"));
    BOOST_CHECK(c.codecTest("\xff\xff\xff", "zzzz"));
    BOOST_CHECK(c.codecTest("Hello there", "G4JgP4wUR4VZQaI"));

    BOOST_CHECK(c.orderTest("", ""));
    BOOST_CHECK(c.orderTest("A", "AA"));
    BOOST_CHECK(c.orderTest("AA", "B"));
    BOOST_CHECK(c.orderTest("apple", "BANANA"));
    BOOST_CHECK(c.orderTest("\x02\x04", "\x03\xff\xff\xff"));

    // Some random strings (GCC is really dumb about embedded hex sequences)
    BOOST_CHECK(c.orderTest("v\x96l", "\xdbH\x18\x17"));
    BOOST_CHECK(c.orderTest("", "\xf7\x91\xf4" "B\x19\xa2~F0"));
    BOOST_CHECK(c.orderTest("\xff\x8b\xf6#\x14Y\x1a" "F1\xee\xdaq\x0e", "\xd9" "B\x18\xa7\xee\xf5\xb7Q_\x84hT\xb5L7"));
    BOOST_CHECK(c.orderTest("\x8f%\x04\xeb\x1eL\xe8>N5\x86", "\xa0\xe1K"));
    BOOST_CHECK(c.orderTest("\x8e\x9eG\x81" "9\x1e\x10#gb", "\xdcj\x16\xfb\xcb\xb8"));
    BOOST_CHECK(c.orderTest("\xf1", "\xcf"));
    BOOST_CHECK(c.orderTest("\x17^t;\x06\x11\xd4\x02\xff\x81\x82\xd3K\x12", "\xd6\x8a"));
    BOOST_CHECK(c.orderTest("0q\xc6\x18g\xfb" "B", "\x14~\xc3JU{\xad?\x88"));
    BOOST_CHECK(c.orderTest("\xbc\xe2\x81-\xf1\xe4\xc4\n\x1d\x03\x8e\x13\x8f\x03\x8d", "\xba\x9c\xd2k6S\x8d"));
    BOOST_CHECK(c.orderTest("/\x06\xae" "D\xf1^Zu\x05" "B", "\xcaV<-C"));
    BOOST_CHECK(c.orderTest("\x86\xbc\x12\xa3\x92\xd3\x9c", "gR\t\xf6\x0b\xdb" "C\xc8nMbX"));
    BOOST_CHECK(c.orderTest("\xd6\xcb\x0f\xc2k\x91=\x85\x17\xcf", "\xd3\xd7\xadG\x10\x89\x94}\xde\xa3\x93\xf7\xbaZ"));
    BOOST_CHECK(c.orderTest("\xe7" "BWP\x8f", "EK;\xc4\\P"));
    BOOST_CHECK(c.orderTest("\xd6\xbeQb\xcag\xc6" "7\x15\xe6\x80", "\xb3"));
    BOOST_CHECK(c.orderTest("I\x0bJ\xd8w\xe4\xb3\xec\xb3\xeb", "\xd1\xa4L\x94" "a}\xb9\""));
}

BOOST_AUTO_UNIT_TEST(strip_decode_test)
{
    Base64Encoder encoder;
    std::string encoded;

    encoder.encode("hello there!", encoded);
    
    Base64Decoder decoder;
    std::string decoded;
    
    decoder.decode(encoded, decoded);
    BOOST_CHECK_EQUAL(decoded, "hello there!");
    
    decoder.decode(encoded+"\n", decoded);
    BOOST_CHECK_EQUAL(decoded, "hello there!");

    decoder.decode(encoded+"\n\n"+encoded, decoded);
    BOOST_CHECK_EQUAL(decoded, "hello there!hello there!");
}
