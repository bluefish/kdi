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

std::string getUniqueTableFile(std::string const & rootDir,
                               std::string const & tableName)
{
    string dir = fs::resolve(rootDir, tableName);

    // XXX: this should be cached -- only need to make the directory
    // once per table
    fs::makedirs(dir);
   
    return File::openUnique(fs::resolve(dir, "$UNIQUE")).second;
}

}
  
BOOST_AUTO_TEST_CASE(compact_test)
{
    DirectBlockCache blockCache;
    Compactor compactor(&blockCache);

    // Make some test fragments
    {
        DiskOutput out(128);
        out.open("memfs:orig_1");
        out.emitCell("row1", "col", 0, "val");
        out.close();
    }

    DiskFragment f1("memfs:orig_1");
    DiskFragment f2("memfs:orig_1");
    DiskFragment f3("memfs:orig_1");
    DiskFragment f4("memfs:orig_1");
    DiskFragment f5("memfs:orig_1");

    RangeFragmentMap candidateSet;
    RangeFragmentMap compactionSet;
    RangeFragmentMap outputSet;

    compactor.chooseCompactionSet(candidateSet, compactionSet);
    compactor.compact(compactionSet, outputSet);
}
