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
#include <kdi/table.h>
#include <kdi/scan_predicate.h>
#include <warp/options.h>
#include <ex/exception.h>
#include <iostream>
#include <boost/format.hpp>

using boost::format;

using namespace kdi;
using namespace kdi::app;
using namespace warp;
using namespace ex;
using namespace std;

class EraseVisitor
    : public NullVisitor
{
public:
    void startTable(string const & uri)
    {
        table = Table::open(uri);
    }

    void endTable()
    {
        table->sync();
        table.reset();
    }

    void visitCell(Cell const & x)
    {
        table->erase(x.getRow(), x.getColumn(), x.getTimestamp());
    }

private:
    TablePtr table;
};


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

    if(dryrun && verbose)
        doScan<CompositeVisitor<VerboseVisitor, CellWriter> >(args, pred);
    else if(dryrun)
        doScan<CellWriter>(args, pred);
    else if(verbose)
        doScan<CompositeVisitor<VerboseVisitor, EraseVisitor> >(args, pred);
    else
        doScan<EraseVisitor>(args, pred);

    return 0;
}
