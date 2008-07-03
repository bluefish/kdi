//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-12-18
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

#include <kdi/table.h>
#include <kdi/scan_predicate.h>
#include <warp/options.h>
#include <ex/exception.h>
#include <iostream>
#include <boost/format.hpp>

using boost::format;

using namespace kdi;
using namespace warp;
using namespace ex;
using namespace std;

//----------------------------------------------------------------------------
// main
//----------------------------------------------------------------------------
int main(int ac, char ** av)
{
    OptionParser op("%prog [options] <table> ...");
    {
        using namespace boost::program_options;
        op.addOption("predicate,p", value<string>(), "Predicate expression for scan");
        op.addOption("dryrun,n", "Print cells that would be erased");
        op.addOption("verbose,v", "Be verbose");
    }

    OptionMap opt;
    ArgumentList args;
    op.parseOrBail(ac, av, opt, args);

    bool verbose = hasopt(opt, "verbose");
    bool dryrun = hasopt(opt, "dryrun");

    ScanPredicate pred;
    {
        string arg;
        if(opt.get("predicate", arg))
        {
            try {
                pred = ScanPredicate(arg);
            }
            catch(ValueError const & ex) {
                op.error(ex.what());
            }
            if(verbose)
                cerr << "using predicate: " << pred << endl;
        }
    }
    
    for(ArgumentList::const_iterator ai = args.begin(); ai != args.end(); ++ai)
    {
        if(verbose)
            cerr << "scanning: " << *ai << endl;

        TablePtr table = Table::open(*ai);
        CellStreamPtr scan = table->scan(pred);
        Cell x;
        size_t nCells = 0;
        size_t nBytes = 0;
        while(scan->get(x))
        {
            // Update counters
            nBytes += x.getRow().size();
            nBytes += x.getColumn().size();
            nBytes += x.getValue().size();
            ++nCells;
            
            // Erase (or print)
            if(dryrun)
                cout << x << endl;
            else
                table->erase(x.getRow(), x.getColumn(), x.getTimestamp());

            // Report
            if(verbose && (nCells % 10000) == 0)
                cerr << format("erased %d cells so far (%sB)")
                    % nCells % sizeString(nBytes) << endl;
        }

        // Sync table
        if(!dryrun)
            table->sync();

        // Report
        if(verbose)
            cerr << format("erased %d cells (%sB)")
                % nCells % sizeString(nBytes) << endl;
    }

    return 0;
}
