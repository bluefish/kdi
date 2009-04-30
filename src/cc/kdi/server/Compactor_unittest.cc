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
#include <kdi/server/DiskFragment.h>
#include <kdi/server/CellBuilder.h>
#include <kdi/server/DirectBlockCache.h>

#include <warp/fs.h>
#include <string>
#include <boost/format.hpp>


using namespace kdi;
using namespace kdi::local;
using namespace kdi::server;
using namespace warp;
using namespace std;
using boost::format;

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

class TestFragMaker : public DiskFragmentMaker
{
public:
    string newDiskFragment()
    {
        string rootDir = "memfs:/";
        string tableName = "test";
        string dir = warp::fs::resolve(rootDir, tableName);
        return File::openUnique(warp::fs::resolve(dir, "$UNIQUE")).second;
    }
};

}

BOOST_AUTO_TEST_CASE(compact_test)
{
    DirectBlockCache blockCache;
    Compactor compactor(&blockCache);

    // Make some test fragments
    {
        DiskWriter out(128);
        out.open("memfs:orig_1");
        out.emitCell("row1", "col", 0, "val");
        out.close();
    }
    
    FragmentCPtr f1(new DiskFragment("memfs:orig_1"));
    FragmentCPtr f2(new DiskFragment("memfs:orig_1"));
    FragmentCPtr f3(new DiskFragment("memfs:orig_1"));
    FragmentCPtr f4(new DiskFragment("memfs:orig_1"));
    FragmentCPtr f5(new DiskFragment("memfs:orig_1"));
    
    RangeFragmentMap candidateSet;
    RangeFragmentMap compactionSet;
    RangeFragmentMap outputSet;

    addFragment(candidateSet, "a", "b", f1);
    addFragment(candidateSet, "a", "b", f2);
    addFragment(candidateSet, "b", "c", f3);
    addFragment(candidateSet, "b", "c", f4);
    addFragment(candidateSet, "b", "c", f5);

    TestFragMaker fragMaker;
    compactor.chooseCompactionSet(candidateSet, compactionSet);
    compactor.compact(compactionSet, &fragMaker, outputSet);
}
