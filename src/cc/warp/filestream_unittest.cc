//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-08-16
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

#include "filestream.h"
#include "memfile.h"
#include "unittest/main.h"
#include <iostream>
#include <boost/iostreams/stream.hpp>

using namespace warp;

BOOST_AUTO_TEST_CASE(test_ostream)
{
    char buf[1024];
    memset(buf, 0, sizeof(buf));

    FilePtr fp(new MemFile(buf, sizeof(buf), true));
    FileStream s(fp);

    s << "Hi there" << std::endl;
    BOOST_CHECK_EQUAL("Hi there\n", buf);

    s.seekg(0);
    s << "Eat ";
    s.flush();

    BOOST_CHECK_EQUAL("Eat here\n", buf);

    std::string x;
    s >> x;

    BOOST_CHECK_EQUAL(x, "here");
}
