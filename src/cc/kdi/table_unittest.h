//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-10-10
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

#ifndef KDI_TABLE_UNITTEST_H
#define KDI_TABLE_UNITTEST_H

#include <kdi/table.h>
#include <kdi/cell.h>
#include <kdi/memory_table.h>
#include <kdi/scan_predicate.h>

#include <unittest/main.h>

#include <boost/test/test_tools.hpp>
#include <boost/test/output_test_stream.hpp>
#include <boost/format.hpp>

namespace kdi {
namespace unittest {

    typedef boost::test_tools::output_test_stream test_out_t;

    test_out_t & operator<<(test_out_t & o, Cell const & x)
    {
        if(x)
        {
            o << '('
              << x.getRow()
              << ','
              << x.getColumn()
              << ','
              << x.getTimestamp()
              << ',';
            if(x.isErasure())
                o << "ERASED";
            else
                o << x.getValue();
            o << ')';
        }
        else
            o << "(NULL-CELL)";
        return o;
    }

    test_out_t & operator<<(test_out_t & o, CellStream & s)
    {
        Cell x;
        while(s.get(x))
            o << x;
        return o;
    }

    test_out_t & operator<<(test_out_t & o, Table const & t)
    {
        o << *t.scan();
        return o;
    }

    /// Fill a Table with cells of the form:
    ///   ("row-i", "col-j", k, "val-i-j-k")
    /// for i in [1, nRows], j in [1, nCols], and k in [1, nRevs]
    void fillTestTable(TablePtr const & tbl, size_t nRows,
                       size_t nCols, size_t nRevs,
                       std::string const & fmt = "%d")
    {
        using boost::format;
        using std::string;

        std::string rowFmt = (format("row-%s") % fmt).str();
        std::string colFmt = (format("col-%s") % fmt).str();
        std::string valFmt = (format("val-%s-%s-%s") % fmt % fmt % fmt).str();

        for(size_t i = 1; i <= nRows; ++i)
        {
            string row = (format(rowFmt)%i).str();
            for(size_t j = 1; j <= nCols; ++j)
            {
                string col = (format(colFmt)%j).str();
                for(size_t k = 1; k <= nRevs; ++k)
                {
                    string val = (format(valFmt)%i%j%k).str();
                    tbl->set(row, col, k, val);
                }
            }
        }
        tbl->sync();
    }

    /// Make a Table containing cells of the form:
    ///   ("row-i", "col-j", k, "val-i-j-k")
    /// for i in [1, nRows], j in [1, nCols], and k in [1, nRevs]
    TablePtr makeTestTable(size_t nRows, size_t nCols, size_t nRevs,
                           std::string const & fmt = "%d")
    {
        TablePtr tbl = MemoryTable::create(true);
        fillTestTable(tbl, nRows, nCols, nRevs);
        return tbl;
    }

    /// Count the number of cells in a scan
    size_t countCells(CellStreamPtr const & scan)
    {
        size_t n = 0;
        Cell x;
        while(scan->get(x))
            ++n;
        return n;
    }

    /// Do basic read/write functionality tests on a Table
    /// implementation.
    void testTableInterface(TablePtr const & table)
    {
        using namespace warp;
        using namespace ex;
        using std::string;

        test_out_t out;

        BOOST_REQUIRE(table);

        // Scan the table and make sure it is empty.  Fail if it is not
        // empty -- we don't want to corrupt some production table
        // somewhere.
        BOOST_REQUIRE((out << *table).is_empty());

        // Put some data in the table
        table->set("r2", "c1", 2, "v212");
        table->set("r3", "c2", 1, "v321");
        table->set("r3", "c0", 1, "v301");
        table->set("r1", "c3", 3, "v133");
        table->set("r1", "c4", 1, "v141");
        table->set("r0", "c0", 2, "v002");
        table->set("r4", "c4", 1, "v441");
        table->set("r3", "c0", 4, "v304");
        table->set("r3", "c1", 1, "v311");
        table->set("r1", "c3", 4, "v134");
        table->set("r4", "c2", 2, "v422");
        table->set("r0", "c2", 0, "v020");
        table->set("r3", "c2", 3, "v323");
        table->set("r0", "c1", 0, "v010");
        table->set("r3", "c0", 1, "v301");
        table->sync();

        // Make sure the data is there
        BOOST_CHECK((out << *table).is_equal(
                        "(r0,c0,2,v002)"
                        "(r0,c1,0,v010)"
                        "(r0,c2,0,v020)"
                        "(r1,c3,4,v134)"
                        "(r1,c3,3,v133)"
                        "(r1,c4,1,v141)"
                        "(r2,c1,2,v212)"
                        "(r3,c0,4,v304)"
                        "(r3,c0,1,v301)"
                        "(r3,c1,1,v311)"
                        "(r3,c2,3,v323)"
                        "(r3,c2,1,v321)"
                        "(r4,c2,2,v422)"
                        "(r4,c4,1,v441)"
                        ));

        // Erase some things (including some data that doesn't exist)
        table->erase("r3", "c0", 3);
        table->erase("r1", "c3", 3);
        table->erase("r3", "c0", 0);
        table->erase("r4", "c2", 4);
        table->erase("r0", "c4", 3);
        table->erase("r3", "c0", 4);
        table->erase("r3", "c2", 0);
        table->erase("r4", "c1", 4);
        table->erase("r1", "c3", 3);
        table->erase("r1", "c4", 4);
        table->erase("r3", "c2", 2);
        table->erase("r1", "c0", 1);
        table->erase("r3", "c0", 4);
        table->erase("r4", "c0", 0);
        table->erase("r1", "c1", 4);
        table->erase("r1", "c3", 3);
        table->erase("r1", "c4", 1);
        table->erase("r0", "c0", 2);
        table->erase("r4", "c4", 1);
        table->sync();

        // Make sure the erased data is gone but the rest is still there
        BOOST_CHECK((out << *table).is_equal(
                        "(r0,c1,0,v010)"
                        "(r0,c2,0,v020)"
                        "(r1,c3,4,v134)"
                        "(r2,c1,2,v212)"
                        "(r3,c0,1,v301)"
                        "(r3,c1,1,v311)"
                        "(r3,c2,3,v323)"
                        "(r3,c2,1,v321)"
                        "(r4,c2,2,v422)"
                        ));

        // Put some more data in the table.  Overwrite some existing
        // cells, add some new cells, re-add some erased cells.
        table->set("r0", "c1", 0, "v010-2");
        table->set("r3", "c0", 1, "v301-2");
        table->set("r3", "c2", 1, "v321-2");
        table->set("r4", "c2", 4, "v424-2");
        table->set("r1", "c3", 3, "v133-2");
        table->set("r0", "c4", 3, "v043-2");
        table->set("r0", "c3", 2, "v032-2");
        table->sync();

        // Make sure the table contains what we expect
        BOOST_CHECK((out << *table).is_equal(
                        "(r0,c1,0,v010-2)"
                        "(r0,c2,0,v020)"
                        "(r0,c3,2,v032-2)"
                        "(r0,c4,3,v043-2)"
                        "(r1,c3,4,v134)"
                        "(r1,c3,3,v133-2)"
                        "(r2,c1,2,v212)"
                        "(r3,c0,1,v301-2)"
                        "(r3,c1,1,v311)"
                        "(r3,c2,3,v323)"
                        "(r3,c2,1,v321-2)"
                        "(r4,c2,4,v424-2)"
                        "(r4,c2,2,v422)"
                        ));

        // Scan the table with various predicates
        BOOST_CHECK((out << *(table->scan(""))).is_equal(
                        "(r0,c1,0,v010-2)"
                        "(r0,c2,0,v020)"
                        "(r0,c3,2,v032-2)"
                        "(r0,c4,3,v043-2)"
                        "(r1,c3,4,v134)"
                        "(r1,c3,3,v133-2)"
                        "(r2,c1,2,v212)"
                        "(r3,c0,1,v301-2)"
                        "(r3,c1,1,v311)"
                        "(r3,c2,3,v323)"
                        "(r3,c2,1,v321-2)"
                        "(r4,c2,4,v424-2)"
                        "(r4,c2,2,v422)"
                        ));

        BOOST_CHECK((out << *(table->scan("history=1"))).is_equal(
                        "(r0,c1,0,v010-2)"
                        "(r0,c2,0,v020)"
                        "(r0,c3,2,v032-2)"
                        "(r0,c4,3,v043-2)"
                        "(r1,c3,4,v134)"
                        "(r2,c1,2,v212)"
                        "(r3,c0,1,v301-2)"
                        "(r3,c1,1,v311)"
                        "(r3,c2,3,v323)"
                        "(r4,c2,4,v424-2)"
                        ));

        BOOST_CHECK((out << *(table->scan("history=-1"))).is_equal(
                        "(r1,c3,3,v133-2)"
                        "(r3,c2,1,v321-2)"
                        "(r4,c2,2,v422)"
                        ));

        BOOST_CHECK((out << *(table->scan("row='r1'"))).is_equal(
                        "(r1,c3,4,v134)"
                        "(r1,c3,3,v133-2)"
                        ));

        BOOST_CHECK((out << *(table->scan("'c1' <= column <= 'c2' and history = 1"))).is_equal(
                        "(r0,c1,0,v010-2)"
                        "(r0,c2,0,v020)"
                        "(r2,c1,2,v212)"
                        "(r3,c1,1,v311)"
                        "(r3,c2,3,v323)"
                        "(r4,c2,4,v424-2)"
                        ));

        BOOST_CHECK((out << *(table->scan("time < @3"))).is_equal(
                        "(r0,c1,0,v010-2)"
                        "(r0,c2,0,v020)"
                        "(r0,c3,2,v032-2)"
                        "(r2,c1,2,v212)"
                        "(r3,c0,1,v301-2)"
                        "(r3,c1,1,v311)"
                        "(r3,c2,1,v321-2)"
                        "(r4,c2,2,v422)"
                        ));
    }

} // namespace unittest
} // namespace kdi

#endif // KDI_TABLE_UNITTEST_H
