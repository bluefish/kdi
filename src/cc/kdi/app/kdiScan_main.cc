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

#include <kdi/table.h>
#include <kdi/scan_predicate.h>
#include <kdi/cell_ostream.h>
#include <warp/options.h>
#include <warp/strutil.h>
#include <warp/timer.h>
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
// VerboseAdapter
//----------------------------------------------------------------------------
template <class V>
class VerboseAdapter
{
    WallTimer totalTimer;
    WallTimer tableTimer;
    
    size_t nCellsTotal;
    size_t nBytesTotal;
    size_t nCells;
    size_t nBytes;
    size_t reportAfter;

    V visitor;

public:
    void startScan()
    {
        totalTimer.reset();
        nCellsTotal = 0;
        nBytesTotal = 0;

        visitor.startScan();
    }

    void endScan()
    {
        double dt = totalTimer.getElapsed();
        cerr << format("%d cells total (%s cells/s, %sB, %sB/s, %.2f sec)")
            % nCellsTotal
            % sizeString(size_t(nCellsTotal / dt), 1000)
            % sizeString(nBytesTotal, 1024)
            % sizeString(size_t(nBytesTotal / dt), 1024)
            % dt
             << endl;

        visitor.endScan();
    }

    void startTable(std::string const & uri)
    {
        cerr << "scanning: " << uri << endl;

        tableTimer.reset();
        nCells = 0;
        nBytes = 0;
        reportAfter = 1 << 20;

        visitor.startTable(uri);
    }

    void endTable()
    {
        double dt = tableTimer.getElapsed();
        cerr << format("%d cells in scan (%s cells/s, %sB, %sB/s, %.2f sec)")
            % nCells
            % sizeString(size_t(nCells / dt), 1000)
            % sizeString(nBytes, 1024)
            % sizeString(size_t(nBytes / dt), 1024)
            % dt
             << endl;

        visitor.endTable();
    }

    void visitCell(Cell const & x)
    {
        size_t sz = (x.getRow().size() + x.getColumn().size() +
                     x.getValue().size() + 8);
        nBytes += sz;
        ++nCells;
            
        if(reportAfter <= sz)
        {
            double dt = tableTimer.getElapsed();
            cerr << format("%d cells so far (%s cells/s, %sB, %sB/s), current row=%s")
                % nCells
                % sizeString(size_t(nCells / dt), 1000)
                % sizeString(nBytes, 1024)
                % sizeString(size_t(nBytes / dt), 1024)
                % reprString(x.getRow())
                 << endl;
            reportAfter = size_t(nBytes * 5 / dt);
        }
        else
            reportAfter -= sz;

        visitor.visitCell(x);
    }
};

//----------------------------------------------------------------------------
// XmlWriter
//----------------------------------------------------------------------------
class XmlWriter
{
public:
    void startScan()
    {
        cout << "<?xml version=\"1.0\"?>" << endl
             << "<cells>" << endl;
    }
    void endScan()
    {
        cout << "</cells>" << endl;
    }

    void startTable(std::string const & uri) {}
    void endTable() {}

    void visitCell(Cell const & x)
    {
        using warp::xml::XmlEscape;

        cout << "<cell>"
             << "<r>" << XmlEscape(x.getRow()) << "</r>"
             << "<c>" << XmlEscape(x.getColumn()) << "</c>"
             << "<t>" << Timestamp::fromMicroseconds(x.getTimestamp()) << "</t>"
             << "<v>" << XmlEscape(x.getValue()) << "</v>"
             << "</cell>" << endl;
    }
};

//----------------------------------------------------------------------------
// CellWriter
//----------------------------------------------------------------------------
class CellWriter
{
public:
    void startScan() {}
    void endScan() {}

    void startTable(std::string const & uri) {}
    void endTable() {}

    void visitCell(Cell const & x)
    {
        cout << x << endl;
    }
};

//----------------------------------------------------------------------------
// FastCellWriter
//----------------------------------------------------------------------------
class FastCellWriter
{
public:
    void startScan() {}
    void endScan() {}

    void startTable(std::string const & uri) {}
    void endTable() {}

    void visitCell(Cell const & x)
    {
        cout << WithFastOutput(x) << endl;
    }
};

//----------------------------------------------------------------------------
// CellCounter
//----------------------------------------------------------------------------
class CellCounter
{
    size_t nCells;

public:
    CellCounter() : nCells(0) {}

    void startScan() {}
    void endScan()
    {
        cout << nCells << endl;
    }

    void startTable(std::string const & uri) {}
    void endTable() {}

    void visitCell(Cell const & x)
    {
        ++nCells;
    }
};

//----------------------------------------------------------------------------
// doScan
//----------------------------------------------------------------------------
template <class V>
void doScan(ArgumentList const & args, ScanPredicate const & pred)
{
    V visitor;

    visitor.startScan();

    for(ArgumentList::const_iterator ai = args.begin();
        ai != args.end(); ++ai)
    {
        visitor.startTable(*ai);

        CellStreamPtr scan = Table::open(*ai)->scan(pred);
        Cell x;
        while(scan->get(x))
            visitor.visitCell(x);

        visitor.endTable();
    }

    visitor.endScan();
}

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
