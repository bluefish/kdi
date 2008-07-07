//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
// Created 2008-05-29
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
