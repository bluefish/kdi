//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: kdi/local/local_table_unittest.cc $
//
// Created 2008/02/05
//
// Copyright 2008 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#include <kdi/local/local_table.h>
#include <kdi/table_unittest.h>
#include <kdi/scan_predicate.h>
#include <unittest/main.h>

using namespace warp;
using namespace kdi;
using namespace kdi::local;
using namespace kdi::unittest;
using namespace std;

BOOST_AUTO_UNIT_TEST(table_interface)
{
    string tableName = "local+memfs:/test";

    // Open the table by name
    TablePtr table = Table::open(tableName);
    BOOST_REQUIRE(table);

    // Test basic interface
    testTableInterface(table);
}
