//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: kdi/meta/meta_table_unittest.cc $
//
// Created 2008/03/19
//
// Copyright 2008 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#include <kdi/table.h>
#include <kdi/meta/meta_util.h>
#include <kdi/table_unittest.h>
#include <warp/uri.h>
#include <unittest/main.h>
#include <ex/exception.h>
#include <string>

using namespace kdi;
using namespace kdi::meta;
using namespace kdi::unittest;
using namespace warp;
using namespace ex;
using namespace std;

namespace {

    struct MetaFixture
    {
        string metaName;
        string tableName;

        MetaFixture(string const & tableName, char const * const * rowSplits) :
            metaName("mem:/meta"),
            tableName(tableName)
        {
            TablePtr metaTable = Table::open(metaName);

            for(char const * const * split = rowSplits; *split; ++split)
            {
                metaTable->set(
                    encodeMetaRow(tableName, *split),
                    "location",
                    0,
                    getTablet(*split)
                    );
            }

            metaTable->set(
                encodeLastMetaRow(tableName),
                "location",
                0,
                getTablet(0)
                );

            metaTable->sync();
        }
    
        /// Open a table using our meta table as an index
        TablePtr openTable() const
        {
            string uri = uriSetParameter("meta+" + metaName, "name",
                                         uriEncode(tableName));
            return Table::open(uri);
        }

        string getTablet(char const * split) const
        {
            if(split)
                return "mem:/tablet/" + tableName + "/last-" + split;
            else
                return "mem:/tablet/" + tableName + "/last";
        }

        TablePtr openTablet(char const * split) const
        {
            return Table::open(getTablet(split));
        }
    };

    string firstRow(CellStreamPtr const & scan)
    {
        Cell x;
        if(!scan->get(x))
            raise<RuntimeError>("empty scan has no first row");
        return str(x.getRow());
    }

    string lastRow(CellStreamPtr const & scan)
    {
        Cell x;
        if(!scan->get(x))
            raise<RuntimeError>("empty scan has no last row");

        {
            Cell y;
            while(scan->get(y))
                x = y;
        }

        return str(x.getRow());
    }
}

BOOST_AUTO_UNIT_TEST(basic)
{
    char const * splits[] = { "row-025", "row-050", "row-075", 0 };
    MetaFixture fix("basic", splits);

    TablePtr tbl = fix.openTable();
    fillTestTable(tbl, 100, 2, 1, "%03d");

    // Make sure the right number of cells are there
    BOOST_CHECK_EQUAL(countCells(tbl->scan()), 200u);

    // Make sure they're distributed as we expect
    TablePtr t1 = fix.openTablet("row-025");
    TablePtr t2 = fix.openTablet("row-050");
    TablePtr t3 = fix.openTablet("row-075");
    TablePtr t4 = fix.openTablet(0);

    BOOST_CHECK_EQUAL(countCells(t1->scan()), 50u);
    BOOST_CHECK_EQUAL(firstRow(t1->scan()), "row-001");
    BOOST_CHECK_EQUAL(lastRow(t1->scan()),  "row-025");

    BOOST_CHECK_EQUAL(countCells(t2->scan()), 50u);
    BOOST_CHECK_EQUAL(firstRow(t2->scan()), "row-026");
    BOOST_CHECK_EQUAL(lastRow(t2->scan()),  "row-050");

    BOOST_CHECK_EQUAL(countCells(t3->scan()), 50u);
    BOOST_CHECK_EQUAL(firstRow(t3->scan()), "row-051");
    BOOST_CHECK_EQUAL(lastRow(t3->scan()),  "row-075");

    BOOST_CHECK_EQUAL(countCells(t4->scan()), 50u);
    BOOST_CHECK_EQUAL(firstRow(t4->scan()), "row-076");
    BOOST_CHECK_EQUAL(lastRow(t4->scan()),  "row-100");

    // Try some predicate scans
    BOOST_CHECK_EQUAL(countCells(tbl->scan("row = 'row-049'")), 2u);
    BOOST_CHECK_EQUAL(countCells(tbl->scan("row = 'row-050'")), 2u);
    BOOST_CHECK_EQUAL(countCells(tbl->scan("row = 'row-051'")), 2u);

    BOOST_CHECK_EQUAL(countCells(tbl->scan("row ~= 'row-01' or"
                                           "row ~= 'row-07'")), 40u);

    BOOST_CHECK_EQUAL(countCells(tbl->scan("row < ''")), 0u);
    BOOST_CHECK_EQUAL(countCells(tbl->scan("row > ''")), 200u);
    BOOST_CHECK_EQUAL(countCells(tbl->scan("'row-001' < row < 'row-100'")), 196u);
    BOOST_CHECK_EQUAL(countCells(tbl->scan("row = 'row-200'")), 0u);
}

BOOST_AUTO_UNIT_TEST(api)
{
    char const * splits[] = { "", "cat", "zebra", "bug", "r1", "r3", 0 };
    MetaFixture fix("api", splits);

    TablePtr tbl = fix.openTable();

    // Only operates on rows in [ "r0" "r4" ]
    testTableInterface(tbl);
}
