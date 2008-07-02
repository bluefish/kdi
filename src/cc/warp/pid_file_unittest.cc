//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: pid_file_unittest.cc $
//
// Created 2007/08/23
//
// Copyright 2007 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#include "pid_file.h"
#include "fs.h"
#include "file.h"
#include "strutil.h"
#include "unittest/main.h"
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
