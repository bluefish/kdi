//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-06-14
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

#include <warp/varsub.h>
#include <unittest/main.h>

using namespace warp;
using namespace std;

BOOST_AUTO_UNIT_TEST(varsub_test)
{
    map<string,string> vars;

    vars["x"] = "marks the spot";
    vars["1"] = "is the loneliest number";
    vars[""] = "empty string!";
    vars["$"] = "dollar";
    vars["dingo"] = "ate your baby";
    vars["thx1138"] = "soma";
    vars["v"] = "$var";
    vars["var"] = "$v";

    BOOST_CHECK_EQUAL(varsub("X $x", vars), "X marks the spot");
    BOOST_CHECK_EQUAL(varsub("X $$x", vars), "X $x");
    BOOST_CHECK_EQUAL(varsub("X $(x)", vars), "X marks the spot");
    BOOST_CHECK_EQUAL(varsub("X ${x}", vars), "X marks the spot");
    BOOST_CHECK_EQUAL(varsub("X $[x]", vars), "X marks the spot");
    BOOST_CHECK_EQUAL(varsub("X $<x>", vars), "X $<x>");

    BOOST_CHECK_EQUAL(varsub("One $1", vars), "One $1");
    BOOST_CHECK_EQUAL(varsub("One $$1", vars), "One $1");
    BOOST_CHECK_EQUAL(varsub("One $(1)", vars), "One is the loneliest number");
    BOOST_CHECK_EQUAL(varsub("One $[1]", vars), "One is the loneliest number");
    BOOST_CHECK_EQUAL(varsub("One ${1}", vars), "One is the loneliest number");
    BOOST_CHECK_EQUAL(varsub("One $<1>", vars), "One $<1>");

    BOOST_CHECK_EQUAL(varsub("No $", vars), "No $");
    BOOST_CHECK_EQUAL(varsub("No $[]", vars), "No empty string!");
    BOOST_CHECK_EQUAL(varsub("No ${}", vars), "No empty string!");
    BOOST_CHECK_EQUAL(varsub("No $()", vars), "No empty string!");

    BOOST_CHECK_EQUAL(varsub("Dollar $", vars), "Dollar $");
    BOOST_CHECK_EQUAL(varsub("Dollar $$", vars), "Dollar $");
    BOOST_CHECK_EQUAL(varsub("Dollar $$$", vars), "Dollar $$");
    BOOST_CHECK_EQUAL(varsub("Dollar $$$$", vars), "Dollar $$");
    BOOST_CHECK_EQUAL(varsub("Dollar $($)", vars), "Dollar dollar");
    BOOST_CHECK_EQUAL(varsub("Dollar $$($)", vars), "Dollar $($)");
    BOOST_CHECK_EQUAL(varsub("Dollar $($$)", vars), "Dollar ");
    BOOST_CHECK_EQUAL(varsub("Dollar $($$)", vars), "Dollar ");
    
    BOOST_CHECK_EQUAL(varsub("k thx $thx1138.", vars), "k thx soma.");
    BOOST_CHECK_EQUAL(varsub("k thx $thx11389.", vars), "k thx .");
    BOOST_CHECK_EQUAL(varsub("k thx ${thx1138}9.", vars), "k thx soma9.");

    BOOST_CHECK_EQUAL(varsub("$v $dingo...", vars), "$var ate your baby...");
}
