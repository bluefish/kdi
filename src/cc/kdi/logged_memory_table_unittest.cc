//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-10-16
// 
// This file is part of KDI.
// 
// KDI is free software; you can redistribute it and/or modify it under the
// terms of the GNU General Public License as published by the Free Software
// Foundation; either version 2 of the License, or any later version.
// 
// KDI is distributed in the hope that it will be useful, but WITHOUT ANY
// WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
// FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
// details.
// 
// You should have received a copy of the GNU General Public License along
// with this program; if not, write to the Free Software Foundation, Inc.,
// 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.
//----------------------------------------------------------------------------

#include <kdi/logged_memory_table.h>
#include <kdi/table_unittest.h>
#include <unittest/main.h>

using namespace kdi;
using namespace kdi::unittest;

BOOST_AUTO_UNIT_TEST(basic_test)
{
    // Create a logged table and put some stuff in it
    {
        // Create table
        TablePtr t = LoggedMemoryTable::create("memfs:log", false);
        
        // Set stuff
        t->set("a", "b:c", 42, "v1");
        t->set("a", "b:c", 45, "v2");
        t->set("x", "y:z", 45, "v3");
        t->set("x", "y:z", 42, "v4");
        t->erase("m", "n:o", 42);

        // Make sure stuff is there
        test_out_t out;
        BOOST_CHECK((out << *t).is_equal(
                        "(a,b:c,45,v2)"
                        "(a,b:c,42,v1)"
                        "(m,n:o,42,ERASED)"
                        "(x,y:z,45,v3)"
                        "(x,y:z,42,v4)"
                        ));

        // Table closes
    }

    // Reload table from same log
    {
        // Create table
        TablePtr t = LoggedMemoryTable::create("memfs:log", false);

        // Make sure stuff is there
        test_out_t out;
        BOOST_CHECK((out << *t).is_equal(
                        "(a,b:c,45,v2)"
                        "(a,b:c,42,v1)"
                        "(m,n:o,42,ERASED)"
                        "(x,y:z,45,v3)"
                        "(x,y:z,42,v4)"
                        ));

        // Add a couple more things
        t->erase("a", "b:c", 42);
        t->set("m", "n:o", 42, "v5");

        // Make sure stuff is there
        BOOST_CHECK((out << *t).is_equal(
                        "(a,b:c,45,v2)"
                        "(a,b:c,42,ERASED)"
                        "(m,n:o,42,v5)"
                        "(x,y:z,45,v3)"
                        "(x,y:z,42,v4)"
                        ));

        // Table closes
    }

    // Reload again, and make sure the updates are still there (and
    // that we didn't lose the old log data)
    {
        // Create table
        TablePtr t = LoggedMemoryTable::create("memfs:log", false);

        // Make sure stuff is there
        test_out_t out;
        BOOST_CHECK((out << *t).is_equal(
                        "(a,b:c,45,v2)"
                        "(a,b:c,42,ERASED)"
                        "(m,n:o,42,v5)"
                        "(x,y:z,45,v3)"
                        "(x,y:z,42,v4)"
                        ));

        // Table closes
    }
}
