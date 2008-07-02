//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/warp/memfile_unittest.cc#2 $
//
// Created 2007/04/13
//
// Copyright 2007 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#include "unittest/main.h"
#include "memfile.h"
#include "ex/exception.h"

extern "C" {
#include <string.h>
}


using namespace warp;
using namespace ex;

BOOST_AUTO_TEST_CASE(const_test)
{
    char writeBuf[100];
    char const readBuf[] = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx";

    // We cannot create writeable files over const data
    MemFile rf1(writeBuf, sizeof(writeBuf), false);
    MemFile rf2(readBuf,  sizeof(readBuf),  false);
    MemFile wf1(writeBuf, sizeof(writeBuf), true);
    BOOST_CHECK_THROW({MemFile wf2(readBuf,  sizeof(readBuf), true);}, ValueError);

    uint32_t x;
    
    // We can read from all files
    BOOST_CHECK_EQUAL(rf1.read(&x, sizeof(x)), sizeof(x));
    BOOST_CHECK_EQUAL(rf2.read(&x, sizeof(x)), sizeof(x));
    BOOST_CHECK_EQUAL(wf1.read(&x, sizeof(x)), sizeof(x));

    // We can only write to writable files
    BOOST_CHECK_THROW(rf1.write(&x, sizeof(x)), IOError);
    BOOST_CHECK_THROW(rf2.write(&x, sizeof(x)), IOError);
    BOOST_CHECK_EQUAL(wf1.write(&x, sizeof(x)), sizeof(x));
}


BOOST_AUTO_TEST_CASE(readline_test)
{
    char const data1[] = "Hello\nworld\n0123456789\nend";
    char line[256];

    MemFile f(data1, strlen(data1));
    
    // Get a line
    BOOST_CHECK_EQUAL(f.readline(line, sizeof(line)), 6u);
    BOOST_CHECK_EQUAL(line, "Hello\n");

    // Get another
    BOOST_CHECK_EQUAL(f.readline(line, sizeof(line)), 6u);
    BOOST_CHECK_EQUAL(line, "world\n");

    // Get a part of a line longer than our buffer
    BOOST_CHECK_EQUAL(f.readline(line, 5), 4u);
    BOOST_CHECK_EQUAL(line, "0123");

    // Get the rest of the line
    BOOST_CHECK_EQUAL(f.readline(line, sizeof(line)), 7u);
    BOOST_CHECK_EQUAL(line, "456789\n");

    // Get last line which has no delimiter
    BOOST_CHECK_EQUAL(f.readline(line, sizeof(line)), 3u);
    BOOST_CHECK_EQUAL(line, "end");

    // Check for EOF
    BOOST_CHECK_EQUAL(f.readline(line, sizeof(line)), 0u);
}

