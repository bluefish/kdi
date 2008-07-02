//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/warp/filestream_unittest.cc#1 $
//
// Created 2007/08/16
//
// Copyright 2007 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
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
