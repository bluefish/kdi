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
#include <kdi/memory_table.h>

#include <kdi/server/FileTracker.h>
#include <kdi/server/PendingFile.h>

#include <warp/fs.h>
#include <string>
#include <boost/format.hpp>
#include <boost/test/test_tools.hpp>
#include <boost/test/output_test_stream.hpp>

#include <kdi/server/DiskWriter.h>

using namespace kdi;
using namespace kdi::local;
using namespace kdi::server;
using namespace warp;
using namespace std;
using boost::format;

namespace {

    typedef boost::shared_ptr<DiskWriter> DiskWriterPtr;
    
    DiskWriterPtr openWriter(std::string const & fn, size_t blockSize)
    {
        static FileTracker tracker("memfs:");
        
        FilePtr fp = File::output(fn);
        PendingFilePtr pfn = tracker.createPending();
        pfn->assignName(fn);
        
        DiskWriterPtr p(new DiskWriter(fp, pfn, blockSize));
        return p;
    }
}

BOOST_AUTO_TEST_CASE(output_test)
{
    DiskWriterPtr out = openWriter("memfs:output", 128);
    BOOST_CHECK_EQUAL(0, out->getCellCount());
    size_t startSize = out->getDataSize();

    out->emitCell("row", "col", 0, "val");
    BOOST_CHECK_EQUAL(1, out->getCellCount());
    BOOST_CHECK(out->getDataSize() > startSize);

    out->emitErasure("erase", "col", 0);
    BOOST_CHECK_EQUAL(2, out->getCellCount());

    PendingFileCPtr fn = out->finish();
    BOOST_CHECK_EQUAL(fn->getName(), "memfs:output");
}

namespace {

typedef std::auto_ptr<FragmentBlock> FragmentBlockPtr;
typedef std::auto_ptr<FragmentBlockReader> FragmentBlockReaderPtr;

void dumpCells(Fragment const & frag, CellOutput & out, string const & pred)
{
    ScanPredicate p(pred);
    size_t blockAddr = frag.nextBlock(p, 0);
    while(blockAddr != size_t(-1))
    {
        FragmentBlockPtr block = frag.loadBlock(blockAddr);
        FragmentBlockReaderPtr reader = block->makeReader(p);

        CellKey nextCell;
        reader->advance(nextCell);
        reader->copyUntil(0, out);
        BOOST_CHECK(!reader->advance(nextCell));

        blockAddr = frag.nextBlock(p, blockAddr+1);
    }
}

void dumpCells(Fragment const & frag, CellOutput & out)
{
    dumpCells(frag, out, "");
}

size_t countCells(Fragment const & frag, string const & pred)
{
    CellBuilder cellBuilder;
    dumpCells(frag, cellBuilder, pred);   
    return cellBuilder.getCellCount();
}

size_t countCells(Fragment const & frag)
{
    return countCells(frag, "");
}

typedef boost::test_tools::output_test_stream test_out_t;

class TestCellOutput : 
    public CellOutput,
    public test_out_t
{
    size_t cellCount;

public:
    TestCellOutput() : cellCount(0) {}
    ~TestCellOutput() {}
        
    void emitCell(strref_t row, strref_t column, int64_t timestamp,
                  strref_t value) 
    {
        *this << '(' << row << ',' << column << ',' << timestamp << ',' 
              << value << ')';
    }

    void emitErasure(strref_t row, strref_t column, int64_t timestamp)
    {
        *this << '(' << row << ',' << column << ',' << timestamp << ',' 
              << "ERASED)";
    }

    size_t getCellCount() const { return cellCount; }
    size_t getDataSize() const { return cellCount; }
};

class TestFragmentBuilder
{
    MemoryTablePtr memTable;
    DiskWriterPtr out;

public:
    
    explicit TestFragmentBuilder(string const & file, size_t blockSz=128) :
        memTable(MemoryTable::create(false)),
        out(openWriter(file,blockSz))
    {
    }

    ~TestFragmentBuilder()
    {
        write();
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

    void write()
    {
        Cell x;
        CellStreamPtr scan = memTable->scan();
        while(scan->get(x))
        {
            if(x.isErasure())
            {
                out->emitErasure(x.getRow(), x.getColumn(), x.getTimestamp());
            }
            else
            {
                out->emitCell(x.getRow(), x.getColumn(), x.getTimestamp(),
                              x.getValue());
            }
        }
        out->finish();
    }
};

/// Fill a fragment with cells of the form:
///   ("row-i", "col-j", k, "val-i-j-k")
/// for i in [1, nRows], j in [1, nCols], and k in [1, nRevs]
void makeTestFragment(size_t blockSize, string const & filename, 
                      size_t nRows, size_t nCols, size_t nRevs,
                      string const & fmt = "%d")
{
    TestFragmentBuilder out(filename, blockSize);

    string rowFmt = (format("row-%s") % fmt).str();
    string colFmt = (format("col-%s") % fmt).str();
    string valFmt = (format("val-%s-%s-%s") % fmt % fmt % fmt).str();

    for(size_t i = 1; i <= nRows; ++i)
    {
        string row = (format(rowFmt)%i).str();
        for(size_t j = 1; j <= nCols; ++j)
        {
            string col = (format(colFmt)%j).str();
            for(size_t k = 1; k <= nRevs; ++k)
            {
                string val = (format(valFmt)%i%j%k).str();
                out.set(row, col, k, val);
            }
        }
    }
}
 
} // namespace

BOOST_AUTO_TEST_CASE(empty_test)
{
    // Make empty table
    {
        DiskWriterPtr out = openWriter("memfs:empty", 128);
        out->finish();
    }

    DiskFragment df("memfs:empty");
    BOOST_CHECK_EQUAL(0, countCells(df));
}

#define CHECK_FRAGMENT(name, expected) \
{ \
    DiskFragment df(name); \
    TestCellOutput test; \
    dumpCells(df, test); \
    BOOST_CHECK(test.is_equal(expected)); \
}

BOOST_AUTO_TEST_CASE(simple_test)
{
    // Write some cells
    {
        DiskWriterPtr out = openWriter("memfs:simple", 128);
        out->emitCell("row1", "col1", 42, "val1");
        out->emitCell("row1", "col2", 42, "val2");
        out->emitCell("row1", "col2", 23, "val3");
        out->emitErasure("row1", "col3", 23);
        out->emitCell("row2", "col1", 42, "val4");
        out->emitCell("row2", "col3", 42, "val5");
        out->emitCell("row3", "col2", 23, "val6");
        out->finish();
    }

    DiskFragment df("memfs:simple");
    BOOST_CHECK_EQUAL(7, countCells(df));

    CHECK_FRAGMENT("memfs:simple",
        "(row1,col1,42,val1)"
        "(row1,col2,42,val2)"
        "(row1,col2,23,val3)"
        "(row1,col3,23,ERASED)"
        "(row2,col1,42,val4)"
        "(row2,col3,42,val5)"
        "(row3,col2,23,val6)"
    );
}

BOOST_AUTO_TEST_CASE(pred_test)
{
    // Write some cells
    {
        DiskWriterPtr out = openWriter("memfs:pred", 128);
        out->emitCell("row1", "col1", 42, "val1");
        out->emitCell("row1", "col2", 42, "val2");
        out->emitCell("row1", "col2", 23, "val3");
        out->emitErasure("row1", "col3", 23);
        out->emitCell("row2", "col1", 42, "val4");
        out->emitCell("row2", "col3", 42, "val5");
        out->emitCell("row3", "col2", 23, "val6");
        out->finish();
    }

    CHECK_FRAGMENT("memfs:pred",
        "(row1,col1,42,val1)"
        "(row1,col2,42,val2)"
        "(row1,col2,23,val3)"
        "(row1,col3,23,ERASED)"
        "(row2,col1,42,val4)"
        "(row2,col3,42,val5)"
        "(row3,col2,23,val6)"
    );
}

BOOST_AUTO_UNIT_TEST(rowscan_test)
{
    makeTestFragment(256, "memfs:rowscan", 1000, 30, 1, "%03d");
    
    DiskFragment df("memfs:rowscan");
    BOOST_CHECK_EQUAL(30000u, countCells(df));
    BOOST_CHECK_EQUAL(60u, countCells(df, "row = 'row-042' or row = 'row-700'"));
    BOOST_CHECK_EQUAL(150u, countCells(df, "row < 'row-004' or row >= 'row-998'"));
    BOOST_CHECK_EQUAL(210u, countCells(df, "'row-442' <  row <  'row-446' or "
                                           "'row-447' <= row <= 'row-450'"));

    // Need to test row scans on tiny tables as well
    // Single block tables can be a corner case
    DiskWriterPtr out = openWriter("memfs:rowscan_small", 2056);
    out->emitCell("row1", "col1", 42, "one1");
    out->emitCell("row2", "col2", 42, "one2");
    out->finish();

    DiskFragment df_small("memfs:rowscan_small");
    BOOST_CHECK_EQUAL(1u, countCells(df_small, "row = 'row2'")); 
}

namespace { 

// Fill a table with cells of the form:
//    ("row-i", "fam-j:qual-k", t, "val-i-j-k-t")
// for i in [1, nRows], j in [1, nFams], k in [1, nQuals] and t in [1, nRevs]
void makeColFamilyTestFragment(size_t blockSize, string const &filename,
                               size_t nRows, size_t nFams, size_t nQuals, size_t nRevs,
                               std::string const &fmt = "%d")
{
    TestFragmentBuilder out(filename, blockSize);

    string rowFmt = (format("row-%s") % fmt).str();
    string colFmt = (format("fam-%s:qual-%s") % fmt % fmt).str();
    string valFmt = (format("val-%s-%s-%s-%s") % fmt % fmt % fmt % fmt).str();

    for(size_t i = 1; i<= nRows; ++i) 
    {
        string row = (format(rowFmt)%i).str();
        for(size_t j = 1; j <= nFams; ++j) 
        {
            for(size_t k = 1; k <= nQuals; ++k) 
            {
                string col = (format(colFmt)%j%k).str();
                for(size_t t = 1; t <= nRevs; ++t)
                {
                    string val = (format(valFmt)%i%j%k%t).str();
                    out.set(row, col, k, val);
                }
            }
        }
    }
}

}

BOOST_AUTO_UNIT_TEST(colscan_test)
{
    makeColFamilyTestFragment(33, "memfs:colscan", 30, 33, 2, 1, "%03d");

    DiskFragment df("memfs:colscan");
    BOOST_CHECK_EQUAL(1980u, countCells(df));
    BOOST_CHECK_EQUAL(0u, countCells(df, "row > 'a' and column ~= 'not-a-fam:'"));
    BOOST_CHECK_EQUAL(60u, countCells(df, "row > 'a' and column ~= 'fam-001:'"));
    BOOST_CHECK_EQUAL(120u, countCells(df, "row > 'a' and column ~= 'fam-001:' or "
                                           "column ~= 'fam-033:'"));
}

BOOST_AUTO_UNIT_TEST(timescan_test)
{
    makeTestFragment(256, "memfs:timescan", 1000, 1, 30, "%03d");

    DiskFragment df("memfs:timescan");
    BOOST_CHECK_EQUAL(30000u, countCells(df));
    BOOST_CHECK_EQUAL(1000u, countCells(df, "row > 'a' and time = @1"));
    BOOST_CHECK_EQUAL(30000u, countCells(df, "row > 'a' and @0 <= time <= @666"));
    BOOST_CHECK_EQUAL(2000u,  countCells(df, "row > 'a' and time = @1 or time = @23"));
}

BOOST_AUTO_UNIT_TEST(filtering_test)
{
    // Try to make verify that filtering blocks doesn't skip data it shouldn't
    
    // Column filtering of the last block of a row predicate with subsequent row predicate
    DiskWriterPtr out = openWriter("memfs:filtering", 1); // Force one cell per block
    out->emitCell("row-A", "fam-1:col", 1, "val");
    out->emitCell("row-A", "fam-2:col", 1, "val");
    out->emitCell("row-Z", "fam-3:col", 1, "val");
    out->emitCell("row-Z", "fam-4:col", 1, "val");
    out->finish();

    DiskFragment df("memfs:filtering");
    BOOST_CHECK_EQUAL(1u, countCells(df, "row = 'row-A' or row = 'row-Z' and " 
                                         "column = 'fam-1:col'"));
    BOOST_CHECK_EQUAL(1u, countCells(df, "row = 'row-A' or row = 'row-Z' and "
                                         "column = 'fam-2:col'"));

}

BOOST_AUTO_UNIT_TEST(advance_test)
{
    // Column filtering of the last block of a row predicate with subsequent 
    // row predicate
    
    DiskWriterPtr out = openWriter("memfs:advance", 4096);  // Make sure they are all in the same block
    out->emitCell("row-A", "col1", 1, "val");
    out->emitCell("row-A", "col2", 1, "val");
    out->emitCell("row-A", "col3", 2, "val");
    out->finish();

    // Get the block out 
    DiskFragment df("memfs:advance");
    size_t blockAddr = df.nextBlock(ScanPredicate(""), 0);
    BOOST_CHECK(blockAddr != size_t(-1));
    FragmentBlockPtr block = df.loadBlock(blockAddr);

    CellKey nextCell;
    FragmentBlockReaderPtr reader; 

    reader = block->makeReader(ScanPredicate("column = 'col2'"));
    BOOST_CHECK(reader->advance(nextCell));
    BOOST_CHECK_EQUAL("col2", nextCell.getColumn());
    
    reader = block->makeReader(ScanPredicate("time = @2"));
    BOOST_CHECK(reader->advance(nextCell));
    BOOST_CHECK_EQUAL(2, nextCell.getTimestamp());
}

BOOST_AUTO_UNIT_TEST(column_family_test)
{
    DiskWriterPtr out = openWriter("memfs:colfam", 16);
    out->emitCell("row-a", "foo:bar", 0, "val-x");
    out->emitCell("row-b", "foo:bar", 0, "val-x");
    out->emitErasure("row-a", "frip:", 0);
    out->emitCell("row-b", "spam:dingo", 0, "val-x");
    out->finish();

    FragmentCPtr f(new DiskFragment("memfs:colfam"));

    BOOST_CHECK(f->getDataSize() > 10u);

    std::vector<std::string> fams;
    f->getColumnFamilies(fams);
    BOOST_CHECK_EQUAL(fams.size(), 3u);
    BOOST_CHECK(std::find(fams.begin(), fams.end(), "foo") != fams.end());
    BOOST_CHECK(std::find(fams.begin(), fams.end(), "frip") != fams.end());
    BOOST_CHECK(std::find(fams.begin(), fams.end(), "spam") != fams.end());
    
    fams.clear();
    fams.push_back("frip");
    FragmentCPtr rf = f->getRestricted(fams);

    fams.clear();
    rf->getColumnFamilies(fams);
    BOOST_CHECK_EQUAL(fams.size(), 1u);
    BOOST_CHECK(std::find(fams.begin(), fams.end(), "frip") != fams.end());

    fams.clear();
    f->getColumnFamilies(fams);
    BOOST_CHECK_EQUAL(fams.size(), 3u);
    BOOST_CHECK(std::find(fams.begin(), fams.end(), "foo") != fams.end());
    BOOST_CHECK(std::find(fams.begin(), fams.end(), "frip") != fams.end());
    BOOST_CHECK(std::find(fams.begin(), fams.end(), "spam") != fams.end());
}
