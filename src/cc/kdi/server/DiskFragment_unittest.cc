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
#include <kdi/server/DiskFragment.h>
#include <kdi/server/CellBuilder.h>

#include <warp/fs.h>
#include <string>
#include <boost/format.hpp>

#include <kdi/local/disk_table.h>
#include <kdi/local/disk_table_writer.h>
#include <kdi/table_unittest.h>
#include <kdi/cell_merge.h>

using namespace kdi;
using namespace kdi::local;
using namespace kdi::server;
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
            DiskTableWriterV1 out(blockSz);
            out.open("memfs:cheater");
            out.close();

            diskTable.reset(new DiskTableV1("memfs:cheater"));
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

                DiskTableWriterV1 out(blockSz);
                out.open("memfs:cheater.tmp");
                Cell x;
                while(merge->get(x))
                    out.put(x);
                out.close();

                diskTable.reset();
                fs::rename("memfs:cheater.tmp", "memfs:cheater", true);
                diskTable.reset(new DiskTableV1("memfs:cheater"));

                memTable = MemoryTable::create(false);
            }
        }
    };

}

BOOST_AUTO_TEST_CASE(empty_test)
{
    // Make empty table
    {
        DiskTableWriterV1 out(128);
        out.open("memfs:empty");
        out.close();
    }

    // Make sure result is empty
    {
        DiskTableV1 t("memfs:empty");
        test_out_t s;
        BOOST_CHECK((s << t).is_empty());
    }

    DiskFragment df("memfs:empty");
    df.nextBlock(ScanPredicate(""), 0);
}

BOOST_AUTO_TEST_CASE(basic_test)
{
    // Write some cells
    {
        DiskTableWriterV1 out(128);
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
        typedef std::auto_ptr<FragmentBlock> FragmentBlockPtr;
        typedef std::auto_ptr<FragmentBlockReader> FragmentBlockReaderPtr;

        ScanPredicate pred("");
        DiskFragment t("memfs:simple");
        CellBuilder cellBuilder;
        for(size_t blockAddr = 0; blockAddr != size_t(-1); 
            blockAddr = t.nextBlock(pred, blockAddr+1))
        {
            FragmentBlockPtr block = t.loadBlock(blockAddr);
            FragmentBlockReaderPtr reader = block->makeReader(pred);

            CellKey nextCell;
            BOOST_CHECK(reader->advance(nextCell));
            reader->copyUntil(0, cellBuilder);
            BOOST_CHECK(!reader->advance(nextCell));
        }

        size_t nCells = cellBuilder.getCellCount();
        BOOST_CHECK_EQUAL(7, nCells);

        CellKey lastKey;
        std::vector<char> out;
        cellBuilder.finish(out, lastKey);

        /*  
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
        */
    }
}

BOOST_AUTO_TEST_CASE(pred_test)
{
    // Write some cells
    {
        DiskTableWriterV1 out(128);
        out.open("memfs:pred");
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
        typedef std::auto_ptr<FragmentBlock> FragmentBlockPtr;
        typedef std::auto_ptr<FragmentBlockReader> FragmentBlockReaderPtr;

        ScanPredicate pred("row = 'row1' or row = 'col3'");
        DiskFragment t("memfs:pred");
        CellBuilder cellBuilder;
        for(size_t blockAddr = 0; blockAddr != size_t(-1); 
            blockAddr = t.nextBlock(pred, blockAddr+1))
        {
            FragmentBlockPtr block = t.loadBlock(blockAddr);
            FragmentBlockReaderPtr reader = block->makeReader(pred);

            CellKey nextCell;
            BOOST_CHECK(reader->advance(nextCell));
            reader->copyUntil(0, cellBuilder);
            BOOST_CHECK(!reader->advance(nextCell));
        }

        size_t nCells = cellBuilder.getCellCount();
        BOOST_CHECK_EQUAL(4, nCells);

        CellKey lastKey;
        std::vector<char> out;
        cellBuilder.finish(out, lastKey);

        /*  
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
        */
    }
}

