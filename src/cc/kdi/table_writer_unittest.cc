//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: kdi/table_writer_unittest.cc $
//
// Created 2008/04/28
//
// Copyright 2008 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#include <kdi/table_writer.h>
#include <kdi/table_unittest.h>
#include <unittest/main.h>

using namespace kdi;
using namespace kdi::unittest;

BOOST_AUTO_UNIT_TEST(cell_stream)
{
    TablePtr t = Table::open("mem:");

    CellStreamPtr cs(new TableWriter(t));

    cs->put(makeCell("a","b",3,"d"));
    cs->put(makeCell("a","b",3,"e"));
    cs->put(makeCell("a","c",2,"d"));
    cs->put(makeCell("a","c",1,"d"));
    cs->flush();

    test_out_t out;

    BOOST_CHECK((out << *(t->scan())).is_equal(
                    "(a,b,3,e)"
                    "(a,c,2,d)"
                    "(a,c,1,d)"
                    ));
}

BOOST_AUTO_UNIT_TEST(row_stream)
{
    TablePtr t = Table::open("mem:");

    RowBufferStreamPtr rs(new TableWriter(t));

    RowBuffer r;
    r.insert(makeCell("a","b",3,"d"));
    r.insert(makeCell("a","b",3,"e"));
    r.insert(makeCell("a","c",2,"d"));
    r.insert(makeCell("a","c",1,"d"));
    rs->put(r);

    r.clear();
    r.insert(makeCell("b","d",3,"a"));
    r.insert(makeCell("b","e",3,"b"));
    r.insert(makeCell("b","d",3,"c"));
    r.insert(makeCell("b","e",3,"d"));
    rs->put(r);

    rs->flush();

    test_out_t out;

    BOOST_CHECK((out << *(t->scan())).is_equal(
                    "(a,b,3,e)"
                    "(a,c,2,d)"
                    "(a,c,1,d)"
                    "(b,d,3,c)"
                    "(b,e,3,d)"
                    ));
}

BOOST_AUTO_UNIT_TEST(both_stream)
{
    TablePtr t = Table::open("mem:");

    boost::shared_ptr<TableWriter> s(new TableWriter(t));

    s->put(makeCell("a","b",3,"d"));
    s->put(makeCell("a","b",3,"e"));
    s->put(makeCell("a","c",2,"d"));
    s->put(makeCell("a","c",1,"d"));

    RowBuffer r;
    r.insert(makeCell("b","d",3,"a"));
    r.insert(makeCell("b","e",3,"b"));
    r.insert(makeCell("b","d",3,"c"));
    r.insert(makeCell("b","e",3,"d"));
    s->put(r);

    s->flush();

    test_out_t out;

    BOOST_CHECK((out << *(t->scan())).is_equal(
                    "(a,b,3,e)"
                    "(a,c,2,d)"
                    "(a,c,1,d)"
                    "(b,d,3,c)"
                    "(b,e,3,d)"
                    ));
}
