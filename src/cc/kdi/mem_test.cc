//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
// Created 2008-08-20
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

#include <kdi/table.h>
#include <warp/options.h>
#include <warp/strutil.h>
#include <boost/format.hpp>
#include <stdlib.h>
#include <string>
#include <iostream>

using namespace kdi;
using namespace warp;
using namespace std;
using boost::format;

namespace
{
    std::string rstring(size_t len)
    {
        uint32_t s = rand();
        std::string r;
        while(len--)
        {
            r += 'a' + (s % 26);
            s = (s << 3) + (s >> 5) + 17;
        }
        return r;
    }
}

int main(int ac, char ** av)
{
    OptionParser op;
    {
        using namespace boost::program_options;
        op.addOption("ncells,n", value<string>(), "number of cells to generate");
    }

    OptionMap opt;
    ArgumentList args;
    op.parseOrBail(ac, av, opt, args);

    string arg;

    size_t nCells = 0;
    if(opt.get("ncells", arg))
        nCells = parseSize(arg);
    
    for(;;)
    {
        TablePtr tbl = Table::open("mem:");
    
        uint32_t r = rand();
        uint32_t c = rand();
        uint32_t t = rand();
        uint32_t v = rand();
    
        size_t nBytes = 0;
        for(size_t i = 0; i < nCells; ++i)
        {
            string row = rstring(10 + (r % 10));
            string col = rstring(10 + (c % 10));
            string val = rstring(50 + (v % 200));

            tbl->set(row, col, t, val);

            r = (r << 3) + (r >> 5) + 17;
            c = (c << 3) + (c >> 5) + 17;
            t = (t << 3) + (t >> 5) + 17;
            v = (v << 3) + (v >> 5) + 17;

            nBytes += row.size() + col.size() + val.size();
        }

        cout << format("Created %s cells (%sB)")
            % sizeString(nCells) % sizeString(nBytes) << endl;
    }
}
