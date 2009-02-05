//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-01-16
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

#include <kdi/local/disk_table.h>
#include <kdi/local/table_types.h>
#include <warp/options.h>
#include <warp/util.h>
#include <tr1/unordered_set>
#include <iostream>
#include <string>

using namespace kdi;
using namespace warp;
using namespace std;

int main(int ac, char ** av)
{
    OptionParser op;

    OptionMap opt;
    ArgumentList args;
    op.parseOrBail(ac, av, opt, args);

    for(ArgumentList::const_iterator i = args.begin();
        i != args.end(); ++i)
    {
        kdi::local::DiskTablePtr dp = kdi::local::DiskTable::loadTable(*i);
        CellStreamPtr scan = dp->scan("");

        Cell x;
        while(scan->get(x)) {
            cout << x.getRow() << endl;
        }
    }

    return 0;
}
