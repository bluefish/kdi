//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/kdi/memory_table_unittest.cc#1 $
//
// Created 2007/09/21
//
// Copyright 2007 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#include <kdi/memory_table.h>
#include <kdi/table_unittest.h>
#include <unittest/main.h>

using namespace kdi;
using namespace kdi::unittest;

BOOST_AUTO_TEST_CASE(basic_table)
{
    test_out_t out;

    MemoryTablePtr t = MemoryTable::create(false);
    BOOST_REQUIRE(t);

    // Make sure table is empty
    BOOST_CHECK((out << *t).is_empty());

    // Insert a cell
    t->set("a", "b:c", 42, "d");
    BOOST_CHECK((out << *t).is_equal(
                    "(a,b:c,42,d)"
                    ));

    // Add another
    t->set("a", "b:c", 45, "e");
    BOOST_CHECK((out << *t).is_equal(
                    "(a,b:c,45,e)"
                    "(a,b:c,42,d)"
                    ));

    // Erase a non-cell
    t->erase("b", "b:c", 45);
    BOOST_CHECK((out << *t).is_equal(
                    "(a,b:c,45,e)"
                    "(a,b:c,42,d)"
                    "(b,b:c,45,ERASED)"
                    ));

    // Erase an existing cell
    t->erase("a", "b:c", 42);
    BOOST_CHECK((out << *t).is_equal(
                    "(a,b:c,45,e)"
                    "(a,b:c,42,ERASED)"
                    "(b,b:c,45,ERASED)"
                    ));
}

BOOST_AUTO_TEST_CASE(basic_api)
{
    TablePtr p = Table::open("mem:");
    testTableInterface(p);
}
