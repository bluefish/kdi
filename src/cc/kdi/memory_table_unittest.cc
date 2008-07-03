//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-09-21
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
