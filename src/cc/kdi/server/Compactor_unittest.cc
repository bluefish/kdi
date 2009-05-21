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
#include <kdi/server/DiskFragment.h>
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

#warning disabled test
// this stuff should be converted to use a simpler test fragment
// structure than DiskFragment.  the mem frag stuff is a possibility
#if 0
BOOST_AUTO_TEST_CASE(compact_test)
{
    FileTracker tracker;
    DiskWriterFactory diskFactory("memfs:", &tracker);

    DirectBlockCache blockCache;
    Compactor compactor(0, &diskFactory, &blockCache);

    TableSchema schema;
    schema.tableName = "test";
    schema.groups.resize(1);
    schema.groups[0].families.push_back("x");

    // Make some test fragments
    {
        DiskWriter out(128);
        out.open("memfs:orig_1");
        out.emitCell("row1", "x:col", 0, "val");
        out.close();
    }
    
    FragmentCPtr f1(new DiskFragment("memfs:orig_1"));
    FragmentCPtr f2(new DiskFragment("memfs:orig_1"));
    FragmentCPtr f3(new DiskFragment("memfs:orig_1"));
    FragmentCPtr f4(new DiskFragment("memfs:orig_1"));
    FragmentCPtr f5(new DiskFragment("memfs:orig_1"));
    
    RangeFragmentMap compactionSet;
    RangeOutputMap outputSet;

    addFragment(compactionSet, "a", "b", f1);
    addFragment(compactionSet, "a", "b", f2);
    addFragment(compactionSet, "b", "c", f3);
    addFragment(compactionSet, "b", "c", f4);
    addFragment(compactionSet, "b", "c", f5);

    compactor.compact(schema, 0, compactionSet, outputSet);
}
#endif

#warning disabled test
#if 0
BOOST_AUTO_TEST_CASE(serialize_test)
{
    FragmentCPtr f1(new DiskFragment("memfs:orig_1"));
    vector<FragmentCPtr> frags;
    frags.push_back(f1);

    TableSchema schema;
    schema.tableName = "test";
    schema.groups.resize(1);
    schema.groups[0].families.push_back("x");

    DiskWriterFactory diskFactory("memfs:");
    boost::scoped_ptr<FragmentWriter> writer(diskFactory.start(schema, 0).release());

    Serializer serialize;
    serialize(schema.groups[0], frags, writer.get());
}
#endif
