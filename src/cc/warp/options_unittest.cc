//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: warp/options_unittest.cc $
//
// Created 2008/06/04
//
// Copyright 2008 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#include <warp/options.h>
#include <unittest/main.h>
#include <string>

using namespace warp;
using namespace std;

BOOST_AUTO_UNIT_TEST(no_quote_stripping)
{
    OptionParser op;
    
    op.addOption("bool,b", "bool option");
    op.addOption("str,s", value<string>(), "string option");

    OptionMap opt;
    ArgumentList args;

    char * av[] = { "prog", "-b", "-s", "'blah'", 0 };
    int ac = sizeof(av) / sizeof(*av) - 1;

    op.parse(ac, av, opt, args);
    
    BOOST_CHECK(hasopt(opt, "bool"));
    
    string s;
    BOOST_CHECK(opt.get("str", s));
    BOOST_CHECK_EQUAL(s, "'blah'");
}
