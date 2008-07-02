//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/warp/varsub_unittest.cc#1 $
//
// Created 2007/06/14
//
// Copyright 2007 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#include "varsub.h"
#include "unittest/main.h"

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
