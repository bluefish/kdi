//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/warp/strutil_unittest.cc#1 $
//
// Created 2007/05/02
//
// Copyright 2007 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#include "unittest/main.h"
#include "strutil.h"
#include "strref.h"
#include "util.h"
#include "ex/exception.h"
#include <string>

using namespace warp;
using namespace ex;
using namespace std;

namespace
{
    string alphaNorm(char const * cStr)
    {
        string s(cStr);
        alphaNormalize(s);
        return s;
    }
}

BOOST_AUTO_TEST_CASE(alpha_normalize)
{
    BOOST_CHECK_EQUAL(alphaNorm("foo"),                         "foo");
    BOOST_CHECK_EQUAL(alphaNorm("foo bar"),                     "foo bar");
    BOOST_CHECK_EQUAL(alphaNorm("Foo   baR"),                   "foo bar");
    BOOST_CHECK_EQUAL(alphaNorm("***FOO--1234--BAR!!!"),        "foo bar");
    BOOST_CHECK_EQUAL(alphaNorm("  How now,\n Brown Cow?\r\n"), "how now brown cow");
    BOOST_CHECK_EQUAL(alphaNorm(""),                            "");
    BOOST_CHECK_EQUAL(alphaNorm("!"),                           "");
    BOOST_CHECK_EQUAL(alphaNorm(">>8!  :)"),                    "");
}


BOOST_AUTO_UNIT_TEST(string_ordering)
{
    char const a[] = "abcd";
    char const b[] = { a[0]+127, a[1]+127, a[2]+127, a[3]+127, 0 };
    char const c[] = "abcde";
    
    str_data_t da(a, a+4);
    str_data_t db(b, b+4);
    str_data_t dc(c, c+5);

    string sa(a, a+4);
    string sb(b, b+4);
    string sc(c, c+5);

    // Make sure self comparison works
    BOOST_CHECK(strcmp(a, a) == 0);
    BOOST_CHECK_EQUAL(lexicographical_compare(a, a+4, a, a+4), false);
    BOOST_CHECK_EQUAL(da < da, false);
    BOOST_CHECK_EQUAL(string_compare(da, da), false);
    BOOST_CHECK_EQUAL(sa < sa, false);
    BOOST_CHECK_EQUAL(lexicographical_compare_3way(a, a+4, a, a+4), 0);
    BOOST_CHECK_EQUAL(string_compare_3way(da, da), 0);



    // Make sure strings get ordered in an unsigned way
    BOOST_CHECK(strcmp(a, b) < 0);
    BOOST_CHECK(strcmp(b, a) > 0);

    // This is a signed comparison
    //BOOST_CHECK_EQUAL(lexicographical_compare(a, a+4, b, b+4), true);
    //BOOST_CHECK_EQUAL(lexicographical_compare(b, b+4, a, a+4), false);

    // str_data_t does signed comparisons
    //BOOST_CHECK_EQUAL(da < db, true);
    //BOOST_CHECK_EQUAL(db < da, false);

    BOOST_CHECK_EQUAL(string_compare(da, db), true);
    BOOST_CHECK_EQUAL(string_compare(db, da), false);

    BOOST_CHECK_EQUAL(sa < sb, true);
    BOOST_CHECK_EQUAL(sb < sa, false);

    // This is a signed comparison
    //BOOST_CHECK(lexicographical_compare_3way(a, a+4, b, b+4) < 0);
    //BOOST_CHECK(lexicographical_compare_3way(b, b+4, a, a+4) > 0);

    BOOST_CHECK(string_compare_3way(da, db) < 0);
    BOOST_CHECK(string_compare_3way(db, da) > 0);



    // Make sure comparisons work where one string is a strict prefix
    // of the other
    BOOST_CHECK(strcmp(a, c) < 0);
    BOOST_CHECK(strcmp(c, a) > 0);

    BOOST_CHECK_EQUAL(lexicographical_compare(a, a+4, c, c+5), true);
    BOOST_CHECK_EQUAL(lexicographical_compare(c, c+5, a, a+4), false);

    BOOST_CHECK_EQUAL(da < dc, true);
    BOOST_CHECK_EQUAL(dc < da, false);

    BOOST_CHECK_EQUAL(string_compare(da, dc), true);
    BOOST_CHECK_EQUAL(string_compare(dc, da), false);

    BOOST_CHECK_EQUAL(sa < sc, true);
    BOOST_CHECK_EQUAL(sc < sa, false);

    BOOST_CHECK(lexicographical_compare_3way(a, a+4, c, c+5) < 0);
    BOOST_CHECK(lexicographical_compare_3way(c, c+5, a, a+4) > 0);

    BOOST_CHECK(string_compare_3way(da, dc) < 0);
    BOOST_CHECK(string_compare_3way(dc, da) > 0);
}

BOOST_AUTO_TEST_CASE(pseudonumeric_order)
{
    PseudoNumericLt lessThan;

    // Normal string comparison
    BOOST_CHECK(lessThan("goodbye", "hello"));
    BOOST_CHECK(!lessThan("hello", "goodbye"));
    BOOST_CHECK(!lessThan("hello", "hello"));

    // Still normal string comparison
    BOOST_CHECK(lessThan("15 things", "3 things"));
    BOOST_CHECK(!lessThan("3 things", "15 things"));

    // Numeric comparison
    BOOST_CHECK(!lessThan("15", "3"));
    BOOST_CHECK(lessThan("3", "15"));
    BOOST_CHECK(!lessThan("3", "3"));

    // Mixed comparison -- numbers before words
    BOOST_CHECK(lessThan("15", "3 things"));
    BOOST_CHECK(lessThan("3", "15 things"));
    BOOST_CHECK(!lessThan("15 things", "3"));
    BOOST_CHECK(!lessThan("3 things", "15"));
}

BOOST_AUTO_TEST_CASE(dehex_test)
{
    BOOST_CHECK_EQUAL(dehex('0','4'), 0x04);
    BOOST_CHECK_EQUAL(dehex('A','E'), 0xae);
    BOOST_CHECK_EQUAL(dehex('a','E'), 0xae);
    BOOST_CHECK_EQUAL(dehex('a','e'), 0xae);
    BOOST_CHECK_EQUAL(dehex('3','3'), 0x33);
    BOOST_CHECK_EQUAL(dehex('g','3'), -1);
    BOOST_CHECK_EQUAL(dehex('3','g'), -1);
}

namespace
{
    string encodeUtf8(uint32_t utf32Char)
    {
        string r;
        warp::encodeUtf8(utf32Char, r);
        return r;
    }

    string decodeEscape(strref_t s)
    {
        string r;
        warp::decodeEscapeSequence(s.begin(), s.end(), r);
        return r;
    }
}

BOOST_AUTO_TEST_CASE(encode_utf8_test)
{
    BOOST_CHECK_EQUAL(encodeUtf8('g'), "g");

    // Just a few characters I encoded in Python
    BOOST_CHECK_EQUAL(encodeUtf8(0x0211), "\xc8\x91");
    BOOST_CHECK_EQUAL(encodeUtf8(0x3311), "\xe3\x8c\x91");
    BOOST_CHECK_EQUAL(encodeUtf8(0x00053311), "\xf1\x93\x8c\x91");

    BOOST_CHECK_THROW(encodeUtf8(0xffffffff), ValueError);
}

BOOST_AUTO_TEST_CASE(decode_escape_sequence_test)
{
    // Common C escapes
    BOOST_CHECK_EQUAL(decodeEscape("r"), "\r");
    BOOST_CHECK_EQUAL(decodeEscape("n"), "\n");
    BOOST_CHECK_EQUAL(decodeEscape("t"), "\t");
    BOOST_CHECK_EQUAL(decodeEscape("\\"), "\\");

    // Self representation for unknown escapes
    BOOST_CHECK_EQUAL(decodeEscape("q"), "q");
    BOOST_CHECK_EQUAL(decodeEscape("g"), "g");
    BOOST_CHECK_EQUAL(decodeEscape("o"), "o");

    // Hex encoding
    BOOST_CHECK_EQUAL(decodeEscape("x44"), "\x44");
    BOOST_CHECK_EQUAL(decodeEscape("x1f"), "\x1f");
    BOOST_CHECK_THROW(decodeEscape("x"),   ValueError);
    BOOST_CHECK_THROW(decodeEscape("x1"),  ValueError);
    BOOST_CHECK_THROW(decodeEscape("x1g"), ValueError);

    // Unicode UTF-8 encoding
    BOOST_CHECK_EQUAL(decodeEscape("u0020"), "\x20");
    BOOST_CHECK_EQUAL(decodeEscape("u0211"), "\xc8\x91");
    BOOST_CHECK_EQUAL(decodeEscape("u3311"), "\xe3\x8c\x91");
    BOOST_CHECK_EQUAL(decodeEscape("U00003311"), "\xe3\x8c\x91");
    BOOST_CHECK_EQUAL(decodeEscape("U00053311"), "\xf1\x93\x8c\x91");

    BOOST_CHECK_THROW(decodeEscape("u"),     ValueError);
    BOOST_CHECK_THROW(decodeEscape("u1"),    ValueError);
    BOOST_CHECK_THROW(decodeEscape("u12"),   ValueError);
    BOOST_CHECK_THROW(decodeEscape("u123"),  ValueError);
    BOOST_CHECK_THROW(decodeEscape("u123g"), ValueError);

    BOOST_CHECK_THROW(decodeEscape("U"),         ValueError);
    BOOST_CHECK_THROW(decodeEscape("U1234"),     ValueError);
    BOOST_CHECK_THROW(decodeEscape("U0000007"),  ValueError);
    BOOST_CHECK_THROW(decodeEscape("U0000007g"), ValueError);
}
