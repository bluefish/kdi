//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-10-09
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

#include <unittest/main.h>
#include <kdi/table_unittest.h>
#include <kdi/memory_table.h>

#include <kdi/local/disk_table.h>
#include <kdi/local/disk_table_writer.h>
#include <warp/fs.h>
#include <string>
#include <boost/format.hpp>

#include <kdi/cell_merge.h>

using namespace kdi;
using namespace kdi::local;
using namespace kdi::unittest;
using namespace warp;
using namespace std;
using boost::format;

namespace {

    class CheaterDiskTable : public Table
    {
        MemoryTablePtr memTable;
        DiskTablePtr diskTable;
        size_t blockSz;

    public:
        explicit CheaterDiskTable(size_t blockSz=128) :
            memTable(MemoryTable::create(false)),
            blockSz(blockSz)
        {
            DiskTableWriter out(128);
            out.open("memfs:cheater");
            out.close();

            diskTable.reset(new DiskTable("memfs:cheater"));
        }

        void set(strref_t row, strref_t column,
                 int64_t timestamp, strref_t value)
        {
            memTable->set(row, column, timestamp, value);
        }

        void erase(strref_t row, strref_t column, int64_t timestamp)
        {
            memTable->erase(row, column, timestamp);
        }

        CellStreamPtr scan(ScanPredicate const & pred) const
        {
            return diskTable->scan(pred);
        }

        void sync()
        {
            if(memTable->getCellCount())
            {
                CellStreamPtr merge = CellMerge::make(true);
                merge->pipeFrom(memTable->scan());
                if(diskTable)
                    merge->pipeFrom(diskTable->scan());

                DiskTableWriter out(blockSz);
                out.open("memfs:cheater.tmp");
                Cell x;
                while(merge->get(x))
                    out.put(x);
                out.close();

                diskTable.reset();
                fs::rename("memfs:cheater.tmp", "memfs:cheater", true);
                diskTable.reset(new DiskTable("memfs:cheater"));

                memTable = MemoryTable::create(false);
            }
        }
    };

}


BOOST_AUTO_TEST_CASE(empty_test)
{
    // Make empty table
    {
        DiskTableWriter out(128);
        out.open("memfs:empty");
        out.close();
    }

    // Make sure result is empty
    {
        DiskTable t("memfs:empty");
        test_out_t s;
        BOOST_CHECK((s << t).is_empty());
    }
}

BOOST_AUTO_TEST_CASE(basic_test)
{
    // Write some cells
    {
        DiskTableWriter out(128);
        out.open("memfs:simple");
        out.put(makeCell("row1", "col1", 42, "val1"));
        out.put(makeCell("row1", "col2", 42, "val2"));
        out.put(makeCell("row1", "col2", 23, "val3"));
        out.put(makeCellErasure("row1", "col3", 23));
        out.put(makeCell("row2", "col1", 42, "val4"));
        out.put(makeCell("row2", "col3", 42, "val5"));
        out.put(makeCell("row3", "col2", 23, "val6"));
        out.close();
    }

    // Make sure those cells are in the table
    {
        DiskTable t("memfs:simple");
        test_out_t s;
        BOOST_CHECK((s << t).is_equal(
                        "(row1,col1,42,val1)"
                        "(row1,col2,42,val2)"
                        "(row1,col2,23,val3)"
                        "(row1,col3,23,ERASED)"
                        "(row2,col1,42,val4)"
                        "(row2,col3,42,val5)"
                        "(row3,col2,23,val6)"
                        )
            );
    }
}

BOOST_AUTO_TEST_CASE(rewrite_test)
{
    DiskTableWriter out(128);

    // Write some cells
    {
        out.open("memfs:one");
        out.put(makeCell("row1", "col1", 42, "one1"));
        out.put(makeCell("row1", "col2", 42, "one2"));
        out.close();
    }

    // Write some more cells
    {
        out.open("memfs:two");
        out.put(makeCell("row1", "col1", 42, "two1"));
        out.put(makeCell("row1", "col3", 42, "two2"));
        out.close();
    }

    // Make sure the cells are in the first table
    {
        DiskTable t("memfs:one");
        test_out_t s;
        BOOST_CHECK((s << t).is_equal(
                        "(row1,col1,42,one1)"
                        "(row1,col2,42,one2)"
                        )
            );
    }

    // Make sure the cells are in the second table
    {
        DiskTable t("memfs:two");
        test_out_t s;
        BOOST_CHECK((s << t).is_equal(
                        "(row1,col1,42,two1)"
                        "(row1,col3,42,two2)"
                        )
            );
    }
}

BOOST_AUTO_TEST_CASE(coarse_test)
{
    // This test assumes that MemoryTable is solid.

    // Basically, we put a bunch of stuff in a MemoryTable, then use
    // DiskTableWriter to serialize it to "disk."  After that, we use
    // the reader side, DiskTable, and verify that it contains the
    // same things we had in the MemoryTable.

    // Fill a MemoryTable with a bunch of stuff.
    MemoryTablePtr t = MemoryTable::create(false);
    size_t nCells = 0;
    for(int ri = 0; ri < 1000; ++ri)
    {
        string row = str(format("row-%d") % (rand() % 20));

        for(int fi = 0; fi < 10; ++fi)
        {
            int family = rand() % 20;
            for(int qi = 0; qi < 10; ++qi)
            {
                string col = str(format("col-%d:%d") % family % (rand() % 20));
                
                for(int ti = 0; ti < 3; ++ti)
                {
                    ++nCells;
                    if(rand() % 10)
                    {
                        t->set(row, col, (rand() % 20),
                               str(format("cell-%d") % nCells));
                    }
                    else
                    {
                        t->erase(row, col, (rand() % 20));
                    }
                }
            }
        }
    }

    // Write it to a file
    size_t nWritten = 0;
    {
        DiskTableWriter diskOut(1<<10);
        diskOut.open("memfs:table");

        Cell x;
        CellStreamPtr scan = t->scan();
        while(scan->get(x))
        {
            diskOut.put(x);
            ++nWritten;
        }

        diskOut.close();
        BOOST_CHECK(fs::filesize("memfs:table") > 0);
    }

    // Read the file and compare with the MemoryTable
    size_t nRead = 0;
    {
        DiskTable diskIn("memfs:table");

        Cell x,y;
        CellStreamPtr scanX = diskIn.scan();
        CellStreamPtr scanY = t->scan();
        for(;;)
        {
            bool gotX = scanX->get(x);
            bool gotY = scanY->get(y);
            
            if(gotX)
                ++nRead;

            if(gotX && gotY)
                BOOST_CHECK_EQUAL(x, y);

            if(!gotX && !gotY)
                break;
        }
    }

    BOOST_CHECK(nRead > 0);
    BOOST_CHECK_EQUAL(nWritten, nRead);
}

BOOST_AUTO_UNIT_TEST(api_test)
{
    TablePtr tbl(new CheaterDiskTable(128));
    testTableInterface(tbl);
}

BOOST_AUTO_UNIT_TEST(rowscan_test)
{
    TablePtr tbl(new CheaterDiskTable(256));
    fillTestTable(tbl, 1000, 30, 1, "%03d");
    
    BOOST_CHECK_EQUAL(countCells(tbl->scan()), 30000u);

    BOOST_CHECK_EQUAL(
        countCells(tbl->scan("row = 'row-042' or row = 'row-700'")),
        60u);

    BOOST_CHECK_EQUAL(
        countCells(tbl->scan("row < 'row-004' or row >= 'row-998'")),
        150u);

    BOOST_CHECK_EQUAL(
        countCells(tbl->scan("'row-442' <  row <  'row-446' or "
                             "'row-447' <= row <= 'row-450'")),
        210u);
}
