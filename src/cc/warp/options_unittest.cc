//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
// Created 2008-06-04
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
