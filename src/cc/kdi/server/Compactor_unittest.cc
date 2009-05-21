//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-10-09
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

#include <unittest/main.h>
#include <kdi/server/Compactor.h>
#include <kdi/server/DiskWriterFactory.h>
#include <kdi/server/DiskWriter.h>
#include <kdi/server/FileTracker.h>
#include <kdi/server/Serializer.h>
#include <kdi/server/TestFragment.h>
#include <kdi/server/DirectBlockCache.h>
#include <kdi/server/CellBuilder.h>
#include <kdi/server/TableSchema.h>
#include <string>

using namespace kdi::server;
using namespace warp;
using namespace std;

namespace {

void addFragment(RangeFragmentMap & rf,
                 string const & minRow,
                 string const & lastRow,
                 FragmentCPtr const & frag)
{
    PointType lower_t = minRow.size() > 0 
        ? PT_EXCLUSIVE_LOWER_BOUND 
        : PT_INFINITE_LOWER_BOUND;
    IntervalPoint<string> lower(minRow, lower_t);

    PointType upper_t = lastRow.size() > 0
        ? PT_INCLUSIVE_UPPER_BOUND 
        : PT_INFINITE_UPPER_BOUND;
    IntervalPoint<string> upper(lastRow, upper_t);

    Interval<string> range(lower, upper);
    rf.addFragment(range, frag);
}

}

BOOST_AUTO_TEST_CASE(compact_test)
{
    FileTracker tracker("memfs:");
    DiskWriterFactory diskFactory("memfs:", &tracker);

    DirectBlockCache blockCache;
    Compactor compactor(0, &diskFactory, &blockCache);

    TableSchema schema;
    schema.tableName = "test";
    schema.groups.resize(1);
    schema.groups[0].families.push_back("x");

    // Make some test fragments
    TestFragmentPtr f(new TestFragment);
    f->set("row1", "x:col", 0, "val");
    
    RangeFragmentMap compactionSet;
    RangeOutputMap outputSet;

    addFragment(compactionSet, "a", "b", f);
    addFragment(compactionSet, "a", "b", f);
    addFragment(compactionSet, "b", "c", f);
    addFragment(compactionSet, "b", "c", f);
    addFragment(compactionSet, "b", "c", f);

    compactor.compact(schema, 0, compactionSet, outputSet);
}
