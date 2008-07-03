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
#include <warp/strutil.h>
#include <warp/xml/xml_escape.h>
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
        op.addOption("xml,x", "Dump as XML");
        op.addOption("count,c", "Only output a count of matching cells");
        op.addOption("verbose,v", "Be verbose");
    }

    OptionMap opt;
    ArgumentList args;
    op.parseOrBail(ac, av, opt, args);

    bool count = hasopt(opt, "count");
    bool verbose = hasopt(opt, "verbose");
    bool dumpXml = hasopt(opt, "xml");
    
    if(count && dumpXml)
        op.error("--count and --xml are incompatible");

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

    if(dumpXml)
        cout << "<?xml version=\"1.0\"?>" << endl
             << "<cells>" << endl;

    size_t nCellsTotal = 0;
    size_t nBytesTotal = 0;
    for(ArgumentList::const_iterator ai = args.begin(); ai != args.end(); ++ai)
    {
        if(verbose)
            cerr << "scanning: " << *ai << endl;
        CellStreamPtr scan = Table::open(*ai)->scan(pred);
        Cell x;
        size_t nCells = 0;
        size_t nBytes = 0;
        while(scan->get(x))
        {
            nBytes += x.getRow().size();
            nBytes += x.getColumn().size();
            nBytes += x.getValue().size();
            ++nCells;
            
            if(verbose && (nCells % 10000) == 0)
                cerr << format("%d cells so far (%sB), current row=%s")
                    % nCells % sizeString(nBytes) % reprString(x.getRow()) << endl;

            if(count)
                continue;

            if(dumpXml)
            {
                using warp::xml::XmlEscape;

                cout << "<cell>"
                     << "<r>" << XmlEscape(x.getRow()) << "</r>"
                     << "<c>" << XmlEscape(x.getColumn()) << "</c>"
                     << "<t>" << x.getTimestamp() << "</t>"
                     << "<v>" << XmlEscape(x.getValue()) << "</v>"
                     << "</cell>" << endl;
            }
            else
                cout << x << endl;
        }
        if(verbose)
            cerr << format("%d cells in scan (%sB)")
                % nCells % sizeString(nBytes) << endl;

        nCellsTotal += nCells;
        nBytesTotal += nBytes;
    }

    if(dumpXml)
        cout << "</cells>" << endl;

    if(count)
        cout << nCellsTotal << endl;

    if(verbose)
        cerr << format("%d cells total (%sB)")
            % nCellsTotal % sizeString(nBytesTotal) << endl;

    return 0;
}
