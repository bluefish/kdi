//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-01-27
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

#ifndef KDI_APP_SCAN_VISITOR_H
#define KDI_APP_SCAN_VISITOR_H

#include <kdi/table.h>
#include <kdi/cell.h>
#include <kdi/cell_ostream.h>
#include <warp/timer.h>
#include <warp/timestamp.h>
#include <warp/repr.h>
#include <warp/strutil.h>
#include <warp/xml/xml_escape.h>
#include <boost/format.hpp>
#include <iostream>
#include <string>
#include <vector>

namespace kdi {
namespace app {

    //------------------------------------------------------------------------
    // VerboseAdapter
    //------------------------------------------------------------------------
    template <class V>
    class VerboseAdapter
    {
        warp::WallTimer totalTimer;
        warp::WallTimer tableTimer;
    
        size_t nCellsTotal;
        size_t nBytesTotal;
        size_t nCells;
        size_t nBytes;
        size_t reportAfter;

        V visitor;

    public:
        explicit VerboseAdapter(V const & v=V()) : visitor(v) {}

        void startScan()
        {
            totalTimer.reset();
            nCellsTotal = 0;
            nBytesTotal = 0;

            visitor.startScan();
        }

        void endScan()
        {
            using boost::format;
            using namespace warp;
            using namespace std;

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
            using namespace std;

            cerr << "scanning: " << uri << endl;

            tableTimer.reset();
            nCells = 0;
            nBytes = 0;
            reportAfter = 1 << 20;

            visitor.startTable(uri);
        }

        void endTable()
        {
            using boost::format;
            using namespace warp;
            using namespace std;

            nCellsTotal += nCells;
            nBytesTotal += nBytes;

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
            using boost::format;
            using namespace warp;
            using namespace std;

            size_t sz = (x.getRow().size() + x.getColumn().size() +
                         x.getValue().size() + 8);
            nBytes += sz;
            ++nCells;
            
            if(reportAfter <= sz)
            {
                double dt = tableTimer.getElapsed();
                cerr << format("%d cells so far (%s cells/s, %sB, %sB/s), current row=\"%s\"")
                    % nCells
                    % sizeString(size_t(nCells / dt), 1000)
                    % sizeString(nBytes, 1024)
                    % sizeString(size_t(nBytes / dt), 1024)
                    % ReprEscape(x.getRow())
                     << endl;
                reportAfter = size_t(nBytes * 5 / dt);
            }
            else
                reportAfter -= sz;

            visitor.visitCell(x);
        }
    };

    //------------------------------------------------------------------------
    // XmlWriter
    //------------------------------------------------------------------------
    class XmlWriter
    {
    public:
        void startScan()
        {
            std::cout << "<?xml version=\"1.0\"?>" << std::endl
                      << "<cells>" << std::endl;
        }
        void endScan()
        {
            std::cout << "</cells>" << std::endl;
        }

        void startTable(std::string const & uri) {}
        void endTable() {}

        void visitCell(Cell const & x)
        {
            using warp::xml::XmlEscape;
            using warp::Timestamp;

            std::cout << "<cell>"
                      << "<r>" << XmlEscape(x.getRow()) << "</r>"
                      << "<c>" << XmlEscape(x.getColumn()) << "</c>"
                      << "<t>" << Timestamp::fromMicroseconds(x.getTimestamp()) << "</t>"
                      << "<v>" << XmlEscape(x.getValue()) << "</v>"
                      << "</cell>" << std::endl;
        }
    };

    //------------------------------------------------------------------------
    // CellWriter
    //------------------------------------------------------------------------
    class CellWriter
    {
    public:
        void startScan() {}
        void endScan() {}

        void startTable(std::string const & uri) {}
        void endTable() {}

        void visitCell(Cell const & x)
        {
            std::cout << x << std::endl;
        }
    };

    //------------------------------------------------------------------------
    // FastCellWriter
    //------------------------------------------------------------------------
    class FastCellWriter
    {
    public:
        void startScan() {}
        void endScan() {}

        void startTable(std::string const & uri) {}
        void endTable() {}

        void visitCell(Cell const & x)
        {
            std::cout << WithFastOutput(x) << std::endl;
        }
    };

    //------------------------------------------------------------------------
    // CellCounter
    //------------------------------------------------------------------------
    class CellCounter
    {
        size_t nCells;

    public:
        CellCounter() : nCells(0) {}

        void startScan() {}
        void endScan()
        {
            std::cout << nCells << std::endl;
        }

        void startTable(std::string const & uri) {}
        void endTable() {}

        void visitCell(Cell const & x)
        {
            ++nCells;
        }
    };

    //------------------------------------------------------------------------
    // AddingVisitor
    //------------------------------------------------------------------------
    class AddingVisitor
    {
    public:
        size_t nCells;
        size_t nBytes;

        AddingVisitor() : nCells(0), nBytes(0) {}

        void startScan() {}
        void endScan() {}

        void startTable(std::string const & uri) {}
        void endTable() {}

        void visitCell(Cell const & x)
        {
            ++nCells;
            nBytes += (8 + x.getRow().size() + x.getColumn().size() +
                       x.getValue().size());
        }
    };

    //------------------------------------------------------------------------
    // doScan
    //------------------------------------------------------------------------
    template <class V>
    void doScan(V & visitor, std::vector<std::string> const & tables,
                ScanPredicate const & pred)
    {
        visitor.startScan();

        for(std::vector<std::string>::const_iterator i = tables.begin();
            i != tables.end(); ++i)
        {
            visitor.startTable(*i);

            CellStreamPtr scan = Table::open(*i)->scan(pred);
            Cell x;
            while(scan->get(x))
                visitor.visitCell(x);

            visitor.endTable();
        }

        visitor.endScan();
    }

    //------------------------------------------------------------------------
    // doScan
    //------------------------------------------------------------------------
    template <class V>
    void doScan(std::vector<std::string> const & tables,
                ScanPredicate const & pred)
    {
        V visitor;
        doScan(visitor, tables, pred);
    }

} // namespace app
} // namespace kdi

#endif // KDI_APP_SCAN_VISITOR_H
