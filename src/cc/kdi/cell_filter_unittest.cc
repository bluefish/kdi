//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-12-27
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

#include <kdi/cell_filter.h>
#include <kdi/table_unittest.h>
#include <warp/interval.h>
#include <unittest/main.h>
#include <string>

using namespace kdi;
using namespace kdi::unittest;
using namespace warp;
using namespace std;

namespace
{
    template <class T>
    boost::shared_ptr< warp::IntervalSet<T> >
    makeSet()
    {
        boost::shared_ptr< warp::IntervalSet<T> > p(
            new warp::IntervalSet<T>
            );
        return p;
    }

    template <class T>
    boost::shared_ptr< warp::IntervalSet<T> >
    makeSet(warp::Interval<T> const & interval1)
    {
        boost::shared_ptr< warp::IntervalSet<T> > p(
            new warp::IntervalSet<T>
            );
        p->add(interval1);
        return p;
    }

    template <class T>
    boost::shared_ptr< warp::IntervalSet<T> >
    makeSet(warp::Interval<T> const & interval1,
            warp::Interval<T> const & interval2)
    {
        boost::shared_ptr< warp::IntervalSet<T> > p(
            new warp::IntervalSet<T>
            );
        p->add(interval1);
        p->add(interval2);
        return p;
    }
}

BOOST_AUTO_UNIT_TEST(predicate_filter_test)
{
    test_out_t out;

    // Set up a table with known contents
    TablePtr tbl = makeTestTable(4, 4, 4);
    BOOST_CHECK_EQUAL(countCells(tbl->scan()), 64u);

    // Apply an empty predicate (full scan)
    {
        ScanPredicate pred;
        CellStreamPtr scan = applyPredicateFilter(pred, tbl->scan());

        BOOST_CHECK_EQUAL(countCells(scan), 64u);
    }

    // Apply predicate for most recent revision of row "row-3"
    {
        CellStreamPtr scan = applyPredicateFilter(
            ScanPredicate()
            .setRowPredicate(
                IntervalSet<string>()
                .add(Interval<string>().setPoint("row-3"))
                )
            .setMaxHistory(1)
            , tbl->scan());

        BOOST_CHECK((out << *scan).is_equal(
                        "(row-3,col-1,4,val-3-1-4)"
                        "(row-3,col-2,4,val-3-2-4)"
                        "(row-3,col-3,4,val-3-3-4)"
                        "(row-3,col-4,4,val-3-4-4)"
                        ));
    }

    // Apply predicate for last two revisions of column "col-1" from all rows
    {
        CellStreamPtr scan = applyPredicateFilter(
            ScanPredicate()
            .setColumnPredicate(
                IntervalSet<string>()
                .add(Interval<string>().setPoint("col-1"))
                )
            .setMaxHistory(2)
            , tbl->scan());

        BOOST_CHECK((out << *scan).is_equal(
                        "(row-1,col-1,4,val-1-1-4)"
                        "(row-1,col-1,3,val-1-1-3)"
                        "(row-2,col-1,4,val-2-1-4)"
                        "(row-2,col-1,3,val-2-1-3)"
                        "(row-3,col-1,4,val-3-1-4)"
                        "(row-3,col-1,3,val-3-1-3)"
                        "(row-4,col-1,4,val-4-1-4)"
                        "(row-4,col-1,3,val-4-1-3)"
                        ));
    }

    // Apply predicate for latest 2 revisions from row "row-2" and
    // column "col-3" before time 4
    {
        CellStreamPtr scan = applyPredicateFilter(
            ScanPredicate()
            .setRowPredicate(
                IntervalSet<string>()
                .add(Interval<string>().setPoint("row-2"))
                )
            .setColumnPredicate(
                IntervalSet<string>()
                .add(Interval<string>().setPoint("col-3"))
                )
            .setTimePredicate(
                IntervalSet<int64_t>()
                .add(
                    Interval<int64_t>()
                    .setUpperBound(4)
                    .unsetLowerBound()
                    )
                )
            .setMaxHistory(2)
            , tbl->scan());

        BOOST_CHECK((out << *scan).is_equal(
                        "(row-2,col-3,3,val-2-3-3)"
                        "(row-2,col-3,2,val-2-3-2)"
                        ));
    }
}

BOOST_AUTO_UNIT_TEST(row_filter_test)
{
    test_out_t out;

    // Set up a table with known contents
    TablePtr tbl = makeTestTable(4, 2, 1);
    BOOST_CHECK_EQUAL(countCells(tbl->scan()), 8u);

    // Make an empty filter -- include nothing
    {
        CellStreamPtr s = makeRowFilter(
            makeSet<string>()
            );
        s->pipeFrom(tbl->scan());

        BOOST_CHECK((out << *s).is_equal(
                        ""
                        ));
    }

    // Make a point filter -- include only "row-2"
    {
        CellStreamPtr s = makeRowFilter(
            makeSet<string>(
                makeInterval<string>(
                    "row-2", "row-2", true, true
                    )
                )
            );
        s->pipeFrom(tbl->scan());

        BOOST_CHECK((out << *s).is_equal(
                        "(row-2,col-1,1,val-2-1-1)"
                        "(row-2,col-2,1,val-2-2-1)"
                        ));
    }

    // Make a range filter -- include ["row-1", "row-3")
    {
        CellStreamPtr s = makeRowFilter(
            makeSet<string>(
                makeInterval<string>(
                    "row-1", "row-3", true, false
                    )
                )
            );
        s->pipeFrom(tbl->scan());

        BOOST_CHECK((out << *s).is_equal(
                        "(row-1,col-1,1,val-1-1-1)"
                        "(row-1,col-2,1,val-1-2-1)"
                        "(row-2,col-1,1,val-2-1-1)"
                        "(row-2,col-2,1,val-2-2-1)"
                        ));
    }

    // Make a two point filter -- include "row-1" and "row-4"
    {
        CellStreamPtr s = makeRowFilter(
            makeSet<string>(
                makeInterval<string>(
                    "row-1", "row-1", true, true
                    ),
                makeInterval<string>(
                    "row-4", "row-4", true, true
                    )
                )
            );
        s->pipeFrom(tbl->scan());

        BOOST_CHECK((out << *s).is_equal(
                        "(row-1,col-1,1,val-1-1-1)"
                        "(row-1,col-2,1,val-1-2-1)"
                        "(row-4,col-1,1,val-4-1-1)"
                        "(row-4,col-2,1,val-4-2-1)"
                        ));
    }
}

BOOST_AUTO_UNIT_TEST(column_filter_test)
{
    test_out_t out;

    // Set up a table with known contents
    TablePtr tbl = makeTestTable(2, 4, 1);
    BOOST_CHECK_EQUAL(countCells(tbl->scan()), 8u);

    // Make an empty filter -- include nothing
    {
        CellStreamPtr s = makeColumnFilter(
            makeSet<string>()
            );
        s->pipeFrom(tbl->scan());

        BOOST_CHECK((out << *s).is_equal(
                        ""
                        ));
    }

    // Make a point filter -- include only "col-2"
    {
        CellStreamPtr s = makeColumnFilter(
            makeSet<string>(
                makeInterval<string>(
                    "col-2", "col-2", true, true
                    )
                )
            );
        s->pipeFrom(tbl->scan());

        BOOST_CHECK((out << *s).is_equal(
                        "(row-1,col-2,1,val-1-2-1)"
                        "(row-2,col-2,1,val-2-2-1)"
                        ));
    }

    // Make a range filter -- include ["col-1", "col-3")
    {
        CellStreamPtr s = makeColumnFilter(
            makeSet<string>(
                makeInterval<string>(
                    "col-1", "col-3", true, false
                    )
                )
            );
        s->pipeFrom(tbl->scan());

        BOOST_CHECK((out << *s).is_equal(
                        "(row-1,col-1,1,val-1-1-1)"
                        "(row-1,col-2,1,val-1-2-1)"
                        "(row-2,col-1,1,val-2-1-1)"
                        "(row-2,col-2,1,val-2-2-1)"
                        ));
    }

    // Make a two point filter -- include "col-1" and "col-4"
    {
        CellStreamPtr s = makeColumnFilter(
            makeSet<string>(
                makeInterval<string>(
                    "col-1", "col-1", true, true
                    ),
                makeInterval<string>(
                    "col-4", "col-4", true, true
                    )
                )
            );
        s->pipeFrom(tbl->scan());

        BOOST_CHECK((out << *s).is_equal(
                        "(row-1,col-1,1,val-1-1-1)"
                        "(row-1,col-4,1,val-1-4-1)"
                        "(row-2,col-1,1,val-2-1-1)"
                        "(row-2,col-4,1,val-2-4-1)"
                        ));
    }
}

BOOST_AUTO_UNIT_TEST(timestamp_filter_test)
{
    test_out_t out;

    // Set up a table with known contents
    TablePtr tbl = makeTestTable(2, 2, 4);
    BOOST_CHECK_EQUAL(countCells(tbl->scan()), 16u);

    // Make an empty filter -- include nothing
    {
        CellStreamPtr s = makeTimestampFilter(
            makeSet<int64_t>()
            );
        s->pipeFrom(tbl->scan());

        BOOST_CHECK((out << *s).is_equal(
                        ""
                        ));
    }

    // Make a point filter -- include only time 3
    {
        CellStreamPtr s = makeTimestampFilter(
            makeSet<int64_t>(
                makeInterval<int64_t>(
                    3, 3, true, true
                    )
                )
            );
        s->pipeFrom(tbl->scan());

        BOOST_CHECK((out << *s).is_equal(
                        "(row-1,col-1,3,val-1-1-3)"
                        "(row-1,col-2,3,val-1-2-3)"
                        "(row-2,col-1,3,val-2-1-3)"
                        "(row-2,col-2,3,val-2-2-3)"
                        ));
    }

    // Make a range filter -- include time [1, 3)
    {
        CellStreamPtr s = makeTimestampFilter(
            makeSet<int64_t>(
                makeInterval<int64_t>(
                    1, 3, true, false
                    )
                )
            );
        s->pipeFrom(tbl->scan());

        BOOST_CHECK((out << *s).is_equal(
                        "(row-1,col-1,2,val-1-1-2)"
                        "(row-1,col-1,1,val-1-1-1)"
                        "(row-1,col-2,2,val-1-2-2)"
                        "(row-1,col-2,1,val-1-2-1)"
                        "(row-2,col-1,2,val-2-1-2)"
                        "(row-2,col-1,1,val-2-1-1)"
                        "(row-2,col-2,2,val-2-2-2)"
                        "(row-2,col-2,1,val-2-2-1)"
                        ));
    }

    // Make a two point filter -- include time 1 and time 4
    {
        CellStreamPtr s = makeTimestampFilter(
            makeSet<int64_t>(
                makeInterval<int64_t>(
                    1, 1, true, true
                    ),
                makeInterval<int64_t>(
                    4, 4, true, true
                    )
                )
            );
        s->pipeFrom(tbl->scan());

        BOOST_CHECK((out << *s).is_equal(
                        "(row-1,col-1,4,val-1-1-4)"
                        "(row-1,col-1,1,val-1-1-1)"
                        "(row-1,col-2,4,val-1-2-4)"
                        "(row-1,col-2,1,val-1-2-1)"
                        "(row-2,col-1,4,val-2-1-4)"
                        "(row-2,col-1,1,val-2-1-1)"
                        "(row-2,col-2,4,val-2-2-4)"
                        "(row-2,col-2,1,val-2-2-1)"
                        ));
    }
}

BOOST_AUTO_UNIT_TEST(history_filter_test)
{
    test_out_t out;

    // Set up a table with known contents
    TablePtr tbl = makeTestTable(2, 2, 4);
    BOOST_CHECK_EQUAL(countCells(tbl->scan()), 16u);

    // Make an empty filter -- include everything
    {
        CellStreamPtr s = makeHistoryFilter(0);
        s->pipeFrom(tbl->scan());

        BOOST_CHECK((out << *s).is_equal(
                        "(row-1,col-1,4,val-1-1-4)"
                        "(row-1,col-1,3,val-1-1-3)"
                        "(row-1,col-1,2,val-1-1-2)"
                        "(row-1,col-1,1,val-1-1-1)"
                        "(row-1,col-2,4,val-1-2-4)"
                        "(row-1,col-2,3,val-1-2-3)"
                        "(row-1,col-2,2,val-1-2-2)"
                        "(row-1,col-2,1,val-1-2-1)"
                        "(row-2,col-1,4,val-2-1-4)"
                        "(row-2,col-1,3,val-2-1-3)"
                        "(row-2,col-1,2,val-2-1-2)"
                        "(row-2,col-1,1,val-2-1-1)"
                        "(row-2,col-2,4,val-2-2-4)"
                        "(row-2,col-2,3,val-2-2-3)"
                        "(row-2,col-2,2,val-2-2-2)"
                        "(row-2,col-2,1,val-2-2-1)"
                        ));
    }

    // Include only the most recent revision
    {
        CellStreamPtr s = makeHistoryFilter(1);
        s->pipeFrom(tbl->scan());

        BOOST_CHECK((out << *s).is_equal(
                        "(row-1,col-1,4,val-1-1-4)"
                        "(row-1,col-2,4,val-1-2-4)"
                        "(row-2,col-1,4,val-2-1-4)"
                        "(row-2,col-2,4,val-2-2-4)"
                        ));
    }

    // Include the most recent 2 revisions
    {
        CellStreamPtr s = makeHistoryFilter(2);
        s->pipeFrom(tbl->scan());

        BOOST_CHECK((out << *s).is_equal(
                        "(row-1,col-1,4,val-1-1-4)"
                        "(row-1,col-1,3,val-1-1-3)"
                        "(row-1,col-2,4,val-1-2-4)"
                        "(row-1,col-2,3,val-1-2-3)"
                        "(row-2,col-1,4,val-2-1-4)"
                        "(row-2,col-1,3,val-2-1-3)"
                        "(row-2,col-2,4,val-2-2-4)"
                        "(row-2,col-2,3,val-2-2-3)"
                        ));
    }

    // Include the most recent 10 revisions
    {
        CellStreamPtr s = makeHistoryFilter(10);
        s->pipeFrom(tbl->scan());

        BOOST_CHECK((out << *s).is_equal(
                        "(row-1,col-1,4,val-1-1-4)"
                        "(row-1,col-1,3,val-1-1-3)"
                        "(row-1,col-1,2,val-1-1-2)"
                        "(row-1,col-1,1,val-1-1-1)"
                        "(row-1,col-2,4,val-1-2-4)"
                        "(row-1,col-2,3,val-1-2-3)"
                        "(row-1,col-2,2,val-1-2-2)"
                        "(row-1,col-2,1,val-1-2-1)"
                        "(row-2,col-1,4,val-2-1-4)"
                        "(row-2,col-1,3,val-2-1-3)"
                        "(row-2,col-1,2,val-2-1-2)"
                        "(row-2,col-1,1,val-2-1-1)"
                        "(row-2,col-2,4,val-2-2-4)"
                        "(row-2,col-2,3,val-2-2-3)"
                        "(row-2,col-2,2,val-2-2-2)"
                        "(row-2,col-2,1,val-2-2-1)"
                        ));
    }

    // Include all but the most recent revision
    {
        CellStreamPtr s = makeHistoryFilter(-1);
        s->pipeFrom(tbl->scan());

        BOOST_CHECK((out << *s).is_equal(
                        "(row-1,col-1,3,val-1-1-3)"
                        "(row-1,col-1,2,val-1-1-2)"
                        "(row-1,col-1,1,val-1-1-1)"
                        "(row-1,col-2,3,val-1-2-3)"
                        "(row-1,col-2,2,val-1-2-2)"
                        "(row-1,col-2,1,val-1-2-1)"
                        "(row-2,col-1,3,val-2-1-3)"
                        "(row-2,col-1,2,val-2-1-2)"
                        "(row-2,col-1,1,val-2-1-1)"
                        "(row-2,col-2,3,val-2-2-3)"
                        "(row-2,col-2,2,val-2-2-2)"
                        "(row-2,col-2,1,val-2-2-1)"
                        ));
    }

    // Include all but the most recent 3 revisions
    {
        CellStreamPtr s = makeHistoryFilter(-3);
        s->pipeFrom(tbl->scan());

        BOOST_CHECK((out << *s).is_equal(
                        "(row-1,col-1,1,val-1-1-1)"
                        "(row-1,col-2,1,val-1-2-1)"
                        "(row-2,col-1,1,val-2-1-1)"
                        "(row-2,col-2,1,val-2-2-1)"
                        ));
    }

    // Include all but the most recent 10 revisions
    {
        CellStreamPtr s = makeHistoryFilter(-10);
        s->pipeFrom(tbl->scan());

        BOOST_CHECK((out << *s).is_equal(
                        ""
                        ));
    }
}
