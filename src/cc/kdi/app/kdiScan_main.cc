//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-12-18
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

#include <kdi/app/scan_visitor.h>
#include <kdi/scan_predicate.h>
#include <warp/options.h>
#include <ex/exception.h>
#include <iostream>

using namespace kdi::app;
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
        op.addOption("numeric,n", "Always print timestamps in numeric form");
    }

    OptionMap opt;
    ArgumentList args;
    op.parseOrBail(ac, av, opt, args);

    bool count = hasopt(opt, "count");
    bool verbose = hasopt(opt, "verbose");
    bool dumpXml = hasopt(opt, "xml");
    bool numericTime = hasopt(opt, "numeric");
    
    if(int(count) + int(dumpXml) + int(numericTime) > 1)
        op.error("--count, --xml, and --numeric are mutually exclusive");

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

    if(verbose)
    {
        if(count)
            doScan<VerboseAdapter<CellCounter> >(args, pred);
        else if(dumpXml)
            doScan<VerboseAdapter<XmlWriter> >(args, pred);
        else if(numericTime)
            doScan<VerboseAdapter<FastCellWriter> >(args, pred);
        else
            doScan<VerboseAdapter<CellWriter> >(args, pred);
    }
    else
    {
        if(count)
            doScan<CellCounter>(args, pred);
        else if(dumpXml)
            doScan<XmlWriter>(args, pred);
        else if(numericTime)
            doScan<FastCellWriter>(args, pred);
        else
            doScan<CellWriter>(args, pred);
    }

    return 0;
}
