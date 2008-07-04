//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-08-23
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

#include <warp/pid_file.h>
#include <warp/fs.h>
#include <warp/file.h>
#include <warp/strutil.h>
#include <unittest/main.h>
#include <unistd.h>
#include <string>

using namespace warp;

BOOST_AUTO_UNIT_TEST(basic_test)
{
    std::string const fn("memfs:/pid");

    // PID file doesn't already exist
    BOOST_CHECK(!fs::exists(fn));

    // Create a PID file
    {
        PidFile pf(fn);

        // PID file was created
        BOOST_CHECK(fs::exists(fn));

        // Make sure it has our PID
        FilePtr fp = File::input(fn);
        char buf[256];
        size_t sz = fp->read(buf, sizeof(buf));
        
        // Chop newline
        while(sz && buf[sz-1] == '\n')
            --sz;

        // Parse PID
        pid_t p(-1);
        BOOST_CHECK(parseInt(p, buf, buf+sz));

        // Make sure it agrees with what it should be
        BOOST_CHECK_EQUAL(getpid(), p);
    }

    // Should be gone again
    BOOST_CHECK(!fs::exists(fn));
}
