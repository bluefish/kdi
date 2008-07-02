//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: warp/md5_unittest.cc $
//
// Created 2008/05/29
//
// Copyright 2008 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#include <warp/md5.h>
#include <unittest/main.h>

using namespace warp;

BOOST_AUTO_UNIT_TEST(hexdigest_test)
{
    // Hashes taken from Python md5.new(x).hexdigest()
    BOOST_CHECK_EQUAL(md5HexDigest(""),               "d41d8cd98f00b204e9800998ecf8427e");
    BOOST_CHECK_EQUAL(md5HexDigest("hello"),          "5d41402abc4b2a76b9719d911017c592");
    BOOST_CHECK_EQUAL(md5HexDigest("some more text"), "4281c348eaf83e70ddce0e07221c3d28");
}
