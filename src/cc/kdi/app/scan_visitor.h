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
    // NullVisitor
    //------------------------------------------------------------------------
    class NullVisitor
    {
    public:
        void startScan() {}
        void endScan() {}

        void startTable(std::string const & uri) {}
        void endTable() {}

        void visitCell(Cell const & x) {}
    };


    //------------------------------------------------------------------------
    // CompositeVisitor
    //------------------------------------------------------------------------
    template <class V1=NullVisitor, class V2=NullVisitor>
    class CompositeVisitor
    {
        V1 v1;
        V2 v2;

    public:
        // GCC 4.1.1 seems unhappy with this constructor when V1
        // and/or V2 is a reference type.  GCC 4.3.0 likes it well
        // enough.  Replacing with the template method and default
        // constructor combo below...

        //explicit CompositeVisitor(V1 const & v1=V1(), V2 const & v2=V2()) :
        //    v1(v1), v2(v2) {}

        CompositeVisitor() {}

        template <class A1, class A2>
        CompositeVisitor(A1 a1, A2 a2) : v1(a1), v2(a2) {}

        void startScan()
        {
            v1.startScan();
            v2.startScan();
        }

        void endScan()
        {
            v1.endScan();
            v2.endScan();
        }

        void startTable(std::string const & uri)
        {
            v1.startTable(uri);
            v2.startTable(uri);
        }

        void endTable()
        {
            v1.endTable();
            v2.endTable();
        }

        void visitCell(Cell const & x)
        {
            v1.visitCell(x);
            v2.visitCell(x);
        }
    };


    //------------------------------------------------------------------------
    // VerboseVisitor
    //------------------------------------------------------------------------
    class VerboseVisitor
    {
        warp::WallTimer totalTimer;
        warp::WallTimer tableTimer;

        size_t nCellsTotal;
        size_t nBytesTotal;
        size_t nCells;
        size_t nBytes;
        size_t reportAfter;

    public:
        void startScan()
        {
            totalTimer.reset();
            nCellsTotal = 0;
            nBytesTotal = 0;
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
        }

        void startTable(std::string const & uri)
        {
            using namespace std;

            cerr << "scanning: " << uri << endl;

            tableTimer.reset();
            nCells = 0;
            nBytes = 0;
            reportAfter = 1 << 20;
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
        }
    };

    //------------------------------------------------------------------------
    // XmlWriter
    //------------------------------------------------------------------------
    class XmlWriter : public NullVisitor
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
    class CellWriter : public NullVisitor
    {
    public:
        void visitCell(Cell const & x)
        {
            std::cout << x << std::endl;
        }
    };

    //------------------------------------------------------------------------
    // FastCellWriter
    //------------------------------------------------------------------------
    class FastCellWriter : public NullVisitor
    {
    public:
        void visitCell(Cell const & x)
        {
            std::cout << WithFastOutput(x) << std::endl;
        }
    };

    //------------------------------------------------------------------------
    // CellCounter
    //------------------------------------------------------------------------
    class CellCounter : public NullVisitor
    {
        size_t nCells;

    public:
        CellCounter() : nCells(0) {}

        void endScan()
        {
            std::cout << nCells << std::endl;
        }

        void visitCell(Cell const & x)
        {
            ++nCells;
        }
    };

    //------------------------------------------------------------------------
    // AddingVisitor
    //------------------------------------------------------------------------
    class AddingVisitor : public NullVisitor
    {
    public:
        size_t nCells;
        size_t nBytes;

        AddingVisitor() : nCells(0), nBytes(0) {}

        void visitCell(Cell const & x)
        {
            ++nCells;
            nBytes += (8 + x.getRow().size() + x.getColumn().size() +
                       x.getValue().size());
        }
    };


    //------------------------------------------------------------------------
    // ReloadingVisitor
    //------------------------------------------------------------------------
    class ReloadingVisitor : public NullVisitor
    {
        double loadRate;
        double syncRate;
        TablePtr curTable;
        bool needSync;

    public:
        size_t nCells;
        size_t nBytes;
        size_t nSyncs;

        ReloadingVisitor(double loadRate, double syncRate) :
            loadRate(loadRate), syncRate(syncRate), needSync(false),
            nCells(0), nBytes(0), nSyncs(0)
        {}

        void startTable(std::string const & uri)
        {
            curTable = Table::open(uri);
        }

        void endTable()
        {
            if(needSync)
            {
                curTable->sync();
                ++nSyncs;
                needSync = false;
            }

            curTable.reset();
        }

        void visitCell(Cell const & x)
        {
            if(rand() < loadRate * RAND_MAX)
            {
                ++nCells;
                nBytes += (8 + x.getRow().size() + x.getColumn().size() +
                           x.getValue().size());

                curTable->insert(x);

                if(rand() < syncRate * RAND_MAX)
                {
                    curTable->sync();
                    ++nSyncs;
                    needSync = false;
                }
                else
                    needSync = true;
            }
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
