//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
// Created 2008-02-05
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
