//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-09-10
// 
// This file is part of the warp library.
// 
// The warp library is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by the
// Free Software Foundation; either version 2 of the License, or any later
// version.
// 
// The warp library is distributed in the hope that it will be useful, but
// WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General
// Public License for more details.
// 
// You should have received a copy of the GNU General Public License along
// with this program; if not, write to the Free Software Foundation, Inc.,
// 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.
//----------------------------------------------------------------------------

#include <warp/interval.h>
#include <warp/functional.h>
#include <unittest/main.h>
#include <boost/test/test_tools.hpp>
#include <sstream>

using namespace warp;

namespace
{
    template <class T, class Lt, class A>
    boost::test_tools::predicate_result
    valid_interval_set(IntervalSet<T,Lt,A> const & s)
    {
        typedef IntervalSet<T,Lt,A> set_t;
        typedef typename set_t::const_iterator citer_t;

        boost::test_tools::predicate_result r(true);

        size_t nPoints = s.size();
        if(nPoints % 2)
        {
            r = false;
            r.message() << "\n  odd number of points: " << nPoints;
        }

        bool haveLast = false;
        PointType lastType = PT_POINT;
        T lastValue = T();
        Lt lt;

        size_t idx = 0;
        for(citer_t i = s.begin(); i != s.end(); ++i, ++idx)
        {
            if(idx % 2)
            {
                if(!i->isUpperBound())
                {
                    r = false;
                    r.message() << "\n  expected upper bound at index " << idx;
                }
            }
            else
            {
                if(!i->isLowerBound())
                {
                    r = false;
                    r.message() << "\n  expected lower bound at index " << idx;
                }
            }

            if(haveLast)
            {
                bool bothFinite = isFinite(lastType) && i->isFinite();
                bool bothExclusive = isExclusive(lastType) && i->isExclusive();
                
                if(bothFinite && lt(i->getValue(), lastValue))
                {
                    r = false;
                    r.message() << "\n  point at index " << idx
                                << " less than predecessor";
                }
                
                if(bothFinite && !bothExclusive && !lt(lastValue, i->getValue()))
                {
                    r = false;
                    r.message() << "\n  unnecessary division at index " << idx;
                }
            }

            lastType = i->getType();
            lastValue = i->getValue();
            haveLast = true;
        }
        return r;
    }

    template <class T, class Lt, class A>
    std::string toString(IntervalSet<T,Lt,A> const & s)
    {
        std::ostringstream oss;
        oss << s;
        return oss.str();
    }
}

BOOST_AUTO_UNIT_TEST(interval_basics)
{
    {
        // Default constructor should produce empty interval
        Interval<int> x;
        BOOST_CHECK(!x.contains(0));
        BOOST_CHECK(!x.contains(1));
    }

    {
        // Check empty setter
        Interval<int> x = Interval<int>().setEmpty();
        BOOST_CHECK(!x.contains(0));
        BOOST_CHECK(!x.contains(1));
    }

    {
        // Check point inclusion
        Interval<int> x = Interval<int>().setPoint(4);
        BOOST_CHECK(!x.contains(3));
        BOOST_CHECK( x.contains(4));
        BOOST_CHECK(!x.contains(5));
    }

    {
        // Check bounded inclusion
        Interval<int> x = Interval<int>().setLowerBound(4).setUpperBound(6);
        BOOST_CHECK(!x.contains(3));
        BOOST_CHECK( x.contains(4));
        BOOST_CHECK( x.contains(5));
        BOOST_CHECK(!x.contains(6));
        BOOST_CHECK(!x.contains(7));

        // Check bounded inclusion
        Interval<int> y = Interval<int>()
            .setLowerBound(4, BT_EXCLUSIVE)
            .setUpperBound(6, BT_INCLUSIVE);
        BOOST_CHECK(!y.contains(3));
        BOOST_CHECK(!y.contains(4));
        BOOST_CHECK( y.contains(5));
        BOOST_CHECK( y.contains(6));
        BOOST_CHECK(!y.contains(7));
    }

    {
        // Check semi-bounded inclusion
        Interval<int> x = Interval<int>().setLowerBound(4).unsetUpperBound();
        BOOST_CHECK(!x.contains(3));
        BOOST_CHECK( x.contains(4));
        BOOST_CHECK( x.contains(5));
        BOOST_CHECK( x.contains(6));
        BOOST_CHECK( x.contains(7));

        // Check semi-bounded inclusion
        Interval<int> y = Interval<int>().unsetLowerBound().setUpperBound(6);
        BOOST_CHECK( y.contains(3));
        BOOST_CHECK( y.contains(4));
        BOOST_CHECK( y.contains(5));
        BOOST_CHECK(!y.contains(6));
        BOOST_CHECK(!y.contains(7));
    }

    {
        // Check unbounded inclusion
        Interval<int> x = Interval<int>().setInfinite();
        BOOST_CHECK( x.contains(3));
        BOOST_CHECK( x.contains(4));
        BOOST_CHECK( x.contains(5));
        BOOST_CHECK( x.contains(6));
        BOOST_CHECK( x.contains(7));
    }

    {
        // Check (non)containment of reversed interval
        Interval<int> x = Interval<int>().setLowerBound(6).setUpperBound(4);
        BOOST_CHECK(!x.contains(3));
        BOOST_CHECK(!x.contains(4));
        BOOST_CHECK(!x.contains(5));
        BOOST_CHECK(!x.contains(6));
        BOOST_CHECK(!x.contains(7));

        // Check again using reversed comparator
        std::greater<int> gt;
        BOOST_CHECK(!x.contains(7, gt));
        BOOST_CHECK( x.contains(6, gt));
        BOOST_CHECK( x.contains(5, gt));
        BOOST_CHECK(!x.contains(4, gt));
        BOOST_CHECK(!x.contains(3, gt));
    }

    {
        // Check isInfinite() and isEmpty() predicates
        std::greater<int> gt;
        Interval<int> x;

        // Default should be empty (under any ordering)
        BOOST_CHECK(!x.isInfinite());
        BOOST_CHECK(x.isEmpty());
        BOOST_CHECK(x.isEmpty(gt));

        // Set a forward range
        x.setLowerBound(10).setUpperBound(30);
        BOOST_CHECK(!x.isInfinite());
        BOOST_CHECK(!x.isEmpty());
        // Still empty on under a reverse ordering
        BOOST_CHECK(x.isEmpty(gt));

        // Set a point
        x.setPoint(7);
        BOOST_CHECK(!x.isInfinite());
        BOOST_CHECK(!x.isEmpty());
        BOOST_CHECK(!x.isEmpty(gt));
        
        // Set to infinite
        x.setInfinite();
        BOOST_CHECK(x.isInfinite());
        BOOST_CHECK(!x.isEmpty());
        BOOST_CHECK(!x.isEmpty(gt));
    }
}

BOOST_AUTO_UNIT_TEST(int_basic)
{
    // Make an empty interval set.  It contains nothing.
    IntervalSet<int> iset;
    BOOST_CHECK(valid_interval_set(iset));

    // Nope, shouldn't contain anything...
    BOOST_CHECK(!iset.contains(42));
    BOOST_CHECK(!iset.contains(makeInterval(35, 40)));
    
    // Add the interval [32,50) to the set
    iset.add(makeInterval(32, 50));
    BOOST_CHECK(valid_interval_set(iset));

    // Check point containment
    BOOST_CHECK(!iset.contains(31));
    BOOST_CHECK( iset.contains(32));
    BOOST_CHECK( iset.contains(42));
    BOOST_CHECK( iset.contains(49));
    BOOST_CHECK(!iset.contains(50));

    // Check interval containment
    BOOST_CHECK( iset.contains(makeInterval(32, 50)));
    BOOST_CHECK( iset.contains(makeInterval(35, 40)));
    BOOST_CHECK(!iset.contains(makeInterval(20, 24)));
    BOOST_CHECK(!iset.contains(makeInterval(30, 33)));
    BOOST_CHECK(!iset.contains(makeInterval(35, 55)));
    BOOST_CHECK(!iset.contains(makeInterval(57, 59)));

    // Check interval overlap
    BOOST_CHECK( iset.overlaps(makeInterval(32, 50)));
    BOOST_CHECK( iset.overlaps(makeInterval(35, 40)));
    BOOST_CHECK(!iset.overlaps(makeInterval(20, 24)));
    BOOST_CHECK( iset.overlaps(makeInterval(30, 33)));
    BOOST_CHECK( iset.overlaps(makeInterval(35, 55)));
    BOOST_CHECK(!iset.overlaps(makeInterval(57, 59)));
}

BOOST_AUTO_UNIT_TEST(int_merge)
{
    IntervalSet<int> iset;
    BOOST_CHECK(valid_interval_set(iset));

    // Add (2 5)  -->  (2 5)
    iset.add(makeInterval(2, 5, false, false));
    BOOST_CHECK(valid_interval_set(iset));
    BOOST_CHECK_EQUAL(iset.size(), 2u);
    BOOST_CHECK(!iset.contains(1));
    BOOST_CHECK(!iset.contains(2));
    BOOST_CHECK( iset.contains(3));
    BOOST_CHECK( iset.contains(4));
    BOOST_CHECK(!iset.contains(5));
    BOOST_CHECK(!iset.contains(6));

    // Add (10 15)  -->  (2 5) (10 15)
    iset.add(makeInterval(10, 15, false, false));
    BOOST_CHECK(valid_interval_set(iset));
    BOOST_CHECK_EQUAL(iset.size(), 4u);
    BOOST_CHECK(!iset.contains(1));
    BOOST_CHECK(!iset.contains(2));
    BOOST_CHECK( iset.contains(3));
    BOOST_CHECK( iset.contains(4));
    BOOST_CHECK(!iset.contains(5));
    BOOST_CHECK(!iset.contains(6));
    BOOST_CHECK(!iset.contains(9));
    BOOST_CHECK(!iset.contains(10));
    BOOST_CHECK( iset.contains(11));
    BOOST_CHECK( iset.contains(14));
    BOOST_CHECK(!iset.contains(15));
    BOOST_CHECK(!iset.contains(16));

    // Add (5 10)  -->  (2 5) (5 10) (10 15)
    iset.add(makeInterval(5, 10, false, false));
    BOOST_CHECK(valid_interval_set(iset));
    BOOST_CHECK_EQUAL(iset.size(), 6u);
    BOOST_CHECK(!iset.contains(1));
    BOOST_CHECK(!iset.contains(2));
    BOOST_CHECK( iset.contains(3));
    BOOST_CHECK( iset.contains(4));
    BOOST_CHECK(!iset.contains(5));
    BOOST_CHECK( iset.contains(6));
    BOOST_CHECK( iset.contains(9));
    BOOST_CHECK(!iset.contains(10));
    BOOST_CHECK( iset.contains(11));
    BOOST_CHECK( iset.contains(14));
    BOOST_CHECK(!iset.contains(15));
    BOOST_CHECK(!iset.contains(16));

    // Add [5 5]  -->  (2 10) (10 15)
    iset.add(makeInterval(5, 5, true, true));
    BOOST_CHECK(valid_interval_set(iset));
    BOOST_CHECK_EQUAL(iset.size(), 4u);
    BOOST_CHECK(!iset.contains(1));
    BOOST_CHECK(!iset.contains(2));
    BOOST_CHECK( iset.contains(3));
    BOOST_CHECK( iset.contains(4));
    BOOST_CHECK( iset.contains(5));
    BOOST_CHECK( iset.contains(6));
    BOOST_CHECK( iset.contains(9));
    BOOST_CHECK(!iset.contains(10));
    BOOST_CHECK( iset.contains(11));
    BOOST_CHECK( iset.contains(14));
    BOOST_CHECK(!iset.contains(15));
    BOOST_CHECK(!iset.contains(16));

    // Add [-INF 13]  -->  [-INF 15)
    iset.add(makeUpperBound(13, true));
    BOOST_CHECK(valid_interval_set(iset));
    BOOST_CHECK_EQUAL(iset.size(), 2u);
    BOOST_CHECK( iset.contains(-100000));
    BOOST_CHECK( iset.contains(1));
    BOOST_CHECK( iset.contains(10));
    BOOST_CHECK( iset.contains(11));
    BOOST_CHECK( iset.contains(14));
    BOOST_CHECK(!iset.contains(15));
    BOOST_CHECK(!iset.contains(16));

    // Add [15 INF]  -->  [-INF INF]
    iset.add(makeLowerBound(15, true));
    BOOST_CHECK(valid_interval_set(iset));
    BOOST_CHECK_EQUAL(iset.size(), 2u);
    BOOST_CHECK( iset.contains(-100000));
    BOOST_CHECK( iset.contains(15));
    BOOST_CHECK( iset.contains(100000));

    // Add (7 23]  -->  [-INF INF]
    iset.add(makeInterval(7, 23, false, true));
    BOOST_CHECK(valid_interval_set(iset));
    BOOST_CHECK_EQUAL(iset.size(), 2u);
    BOOST_CHECK( iset.contains(-100000));
    BOOST_CHECK( iset.contains(15));
    BOOST_CHECK( iset.contains(100000));
}

BOOST_AUTO_UNIT_TEST(intervalset_hull)
{
    IntervalSet<int> iset;
    BOOST_CHECK(iset.getHull().isEmpty());
    
    iset.add(Interval<int>().setLowerBound(4).setUpperBound(10));
    BOOST_CHECK(iset.getHull().getLowerBound().isInclusive());
    BOOST_CHECK(iset.getHull().getLowerBound().getValue() == 4);
    BOOST_CHECK(iset.getHull().getUpperBound().isExclusive());
    BOOST_CHECK(iset.getHull().getUpperBound().getValue() == 10);

    iset.add(Interval<int>().setLowerBound(14).setUpperBound(20));
    BOOST_CHECK(iset.getHull().getLowerBound().isInclusive());
    BOOST_CHECK(iset.getHull().getLowerBound().getValue() == 4);
    BOOST_CHECK(iset.getHull().getUpperBound().isExclusive());
    BOOST_CHECK(iset.getHull().getUpperBound().getValue() == 20);

    iset.add(Interval<int>().setLowerBound(6).unsetUpperBound());
    BOOST_CHECK(iset.getHull().getLowerBound().isInclusive());
    BOOST_CHECK(iset.getHull().getLowerBound().getValue() == 4);
    BOOST_CHECK(iset.getHull().getUpperBound().isInfinite());

    iset.add(Interval<int>().unsetLowerBound().setUpperBound(15));
    BOOST_CHECK(iset.getHull().getLowerBound().isInfinite());
    BOOST_CHECK(iset.getHull().getUpperBound().isInfinite());
}

BOOST_AUTO_UNIT_TEST(interval_contains_interval)
{
    // Empty intervals contain nothing
    BOOST_CHECK_EQUAL(Interval<int>().contains(makeInterval<int>(1,5)), false);
    BOOST_CHECK_EQUAL(Interval<int>().contains(Interval<int>()), false);
    
    // Non-empty intervals contain all empty intervals
    BOOST_CHECK_EQUAL(makeInterval<int>(1,2).contains(Interval<int>()), true);
    BOOST_CHECK_EQUAL(makeInterval<int>(1,2).contains(makeInterval<int>(-3,-4)), true);

    // Intervals contain all sub-intervals
    BOOST_CHECK_EQUAL(makeInterval<int>(1,9).contains(makeInterval<int>(2,5)), true);
    BOOST_CHECK_EQUAL(makeInterval<int>(1,9).contains(makeInterval<int>(5,9)), true);
    BOOST_CHECK_EQUAL(makeInterval<int>(1,9).contains(makeInterval<int>(1,4)), true);
    BOOST_CHECK_EQUAL(makeInterval<int>(1,9).contains(makeInterval<int>(1,9)), true);

    // Intervals do not contain partial overlaps or disjoint intervals
    BOOST_CHECK_EQUAL(makeInterval<int>(1,9).contains(makeInterval<int>(-2,5)), false);
    BOOST_CHECK_EQUAL(makeInterval<int>(1,9).contains(makeInterval<int>(5,10)), false);
    BOOST_CHECK_EQUAL(makeInterval<int>(1,9).contains(makeInterval<int>(-2,20)), false);
    BOOST_CHECK_EQUAL(makeInterval<int>(1,9).contains(makeInterval<int>(20,30)), false);
    BOOST_CHECK_EQUAL(makeInterval<int>(1,9).contains(makeInterval<int>(-30,-20)), false);
    
    // Containment respects bound types
    BOOST_CHECK_EQUAL(makeInterval<int>(1,9,true,true).contains(makeInterval<int>(1,9,true,true)), true);
    BOOST_CHECK_EQUAL(makeInterval<int>(1,9,true,true).contains(makeInterval<int>(1,9,true,false)), true);
    BOOST_CHECK_EQUAL(makeInterval<int>(1,9,true,true).contains(makeInterval<int>(1,9,false,true)), true);
    BOOST_CHECK_EQUAL(makeInterval<int>(1,9,true,true).contains(makeInterval<int>(1,9,false,false)), true);

    BOOST_CHECK_EQUAL(makeInterval<int>(1,9,true,false).contains(makeInterval<int>(1,9,true,true)), false);
    BOOST_CHECK_EQUAL(makeInterval<int>(1,9,true,false).contains(makeInterval<int>(1,9,true,false)), true);
    BOOST_CHECK_EQUAL(makeInterval<int>(1,9,true,false).contains(makeInterval<int>(1,9,false,true)), false);
    BOOST_CHECK_EQUAL(makeInterval<int>(1,9,true,false).contains(makeInterval<int>(1,9,false,false)), true);

    BOOST_CHECK_EQUAL(makeInterval<int>(1,9,false,true).contains(makeInterval<int>(1,9,true,true)), false);
    BOOST_CHECK_EQUAL(makeInterval<int>(1,9,false,true).contains(makeInterval<int>(1,9,true,false)), false);
    BOOST_CHECK_EQUAL(makeInterval<int>(1,9,false,true).contains(makeInterval<int>(1,9,false,true)), true);
    BOOST_CHECK_EQUAL(makeInterval<int>(1,9,false,true).contains(makeInterval<int>(1,9,false,false)), true);

    BOOST_CHECK_EQUAL(makeInterval<int>(1,9,false,false).contains(makeInterval<int>(1,9,true,true)), false);
    BOOST_CHECK_EQUAL(makeInterval<int>(1,9,false,false).contains(makeInterval<int>(1,9,true,false)), false);
    BOOST_CHECK_EQUAL(makeInterval<int>(1,9,false,false).contains(makeInterval<int>(1,9,false,true)), false);
    BOOST_CHECK_EQUAL(makeInterval<int>(1,9,false,false).contains(makeInterval<int>(1,9,false,false)), true);
    
    // We can use custom comparators too
    warp::less wlt;
    std::greater<int> gt;

    BOOST_CHECK_EQUAL(makeInterval<int>(9,1).contains(makeInterval<int>(4,3), wlt), false);
    BOOST_CHECK_EQUAL(makeInterval<int>(9,1).contains(makeInterval<int>(4,3), gt), true);

    BOOST_CHECK_EQUAL(makeInterval<int>(1,5).contains(makeInterval<float>(1.1f,1.4f), wlt), true);
    BOOST_CHECK_EQUAL(makeInterval<int>(1,5).contains(makeInterval<float>(0.9f,1.4f), wlt), false);
}

BOOST_AUTO_UNIT_TEST(interval_overlaps_interval)
{
    // Empty intervals overlap nothing
    BOOST_CHECK_EQUAL(Interval<int>().overlaps(Interval<int>()), false);
    BOOST_CHECK_EQUAL(Interval<int>().overlaps(makeInterval<int>(1,5)), false);
    BOOST_CHECK_EQUAL(makeInterval<int>(1,5).overlaps(Interval<int>()), false);
    
    // Intervals overlap contained intervals
    BOOST_CHECK_EQUAL(makeInterval<int>(1,9).overlaps(makeInterval<int>(2,5)), true);
    BOOST_CHECK_EQUAL(makeInterval<int>(1,9).overlaps(makeInterval<int>(5,9)), true);
    BOOST_CHECK_EQUAL(makeInterval<int>(1,9).overlaps(makeInterval<int>(1,4)), true);
    BOOST_CHECK_EQUAL(makeInterval<int>(1,9).overlaps(makeInterval<int>(1,9)), true);

    // Intervals overlap containing intervals
    BOOST_CHECK_EQUAL(makeInterval<int>(2,4).overlaps(makeInterval<int>(1,5)), true);

    // Check partial overlaps
    BOOST_CHECK_EQUAL(makeInterval<int>(1,9).overlaps(makeInterval<int>(-2,5)), true);
    BOOST_CHECK_EQUAL(makeInterval<int>(1,9).overlaps(makeInterval<int>(5,10)), true);

    // Disjoint intervals do not overlap
    BOOST_CHECK_EQUAL(makeInterval<int>(1,9).overlaps(makeInterval<int>(20,30)), false);
    BOOST_CHECK_EQUAL(makeInterval<int>(1,9).overlaps(makeInterval<int>(-30,-20)), false);
    
    // Overlapping respects bound types
    BOOST_CHECK_EQUAL(makeInterval<int>(1,5,true,true).overlaps(makeInterval<int>(5,9,true,true)), true);
    BOOST_CHECK_EQUAL(makeInterval<int>(1,5,true,false).overlaps(makeInterval<int>(5,9,true,true)), false);
    BOOST_CHECK_EQUAL(makeInterval<int>(1,5,true,false).overlaps(makeInterval<int>(5,9,false,true)), false);
    BOOST_CHECK_EQUAL(makeInterval<int>(1,5,true,true).overlaps(makeInterval<int>(5,9,false,true)), false);

    BOOST_CHECK_EQUAL(makeInterval<int>(5,9,true,true).overlaps(makeInterval<int>(1,5,true,true)), true);
    BOOST_CHECK_EQUAL(makeInterval<int>(5,9,false,true).overlaps(makeInterval<int>(1,5,true,true)), false);
    BOOST_CHECK_EQUAL(makeInterval<int>(5,9,false,true).overlaps(makeInterval<int>(1,5,true,false)), false);
    BOOST_CHECK_EQUAL(makeInterval<int>(5,9,true,true).overlaps(makeInterval<int>(1,5,true,false)), false);
    
    // We can use custom comparators too
    warp::less wlt;
    std::greater<int> gt;

    BOOST_CHECK_EQUAL(makeInterval<int>(9,1).overlaps(makeInterval<int>(4,3), wlt), false);
    BOOST_CHECK_EQUAL(makeInterval<int>(9,1).overlaps(makeInterval<int>(4,3), gt), true);

    BOOST_CHECK_EQUAL(makeInterval<int>(1,5).overlaps(makeInterval<float>(1.1f,1.4f), wlt), true);
    BOOST_CHECK_EQUAL(makeInterval<int>(1,5).overlaps(makeInterval<float>(0.9f,1.4f), wlt), true);
}

BOOST_AUTO_UNIT_TEST(intervalset_clip)
{
    // Test set:  [1 7) (7 10) (13 21] [22 >>
    IntervalSet<int> testSet;
    testSet
        .add(makeInterval<int>(1, 7, true, false))
        .add(makeInterval<int>(7, 10, false, false))
        .add(makeInterval<int>(13, 21, false, true))
        .add(makeLowerBound<int>(22, true))
        ;
    BOOST_CHECK_EQUAL(toString(testSet), "[1 7) (7 10) (13 21] [22 >>");

    {
        // Clip region internal to one interval
        // Clip: [4 5] --> [4 5]
        IntervalSet<int> clipSet(testSet);
        clipSet.clip(makeInterval<int>(4, 5, true, true));
        BOOST_CHECK_EQUAL(toString(clipSet), "[4 5]");
    }

    {
        // No intersection with clip region
        // Clip: (-3 1) --> EMPTY
        IntervalSet<int> clipSet(testSet);
        clipSet.clip(makeInterval<int>(-3, 1, false, false));
        BOOST_CHECK_EQUAL(toString(clipSet), "");
    }

    {
        // Clip region intersects two regions
        // Clip: (-3 8] --> [1 7) (7 8]
        IntervalSet<int> clipSet(testSet);
        clipSet.clip(makeInterval<int>(-3, 8, false, true));
        BOOST_CHECK_EQUAL(toString(clipSet), "[1 7) (7 8]");
    }

    {
        // Empty clip region
        // Clip: (-3 -8) --> EMPTY
        IntervalSet<int> clipSet(testSet);
        clipSet.clip(makeInterval<int>(-3, -8, false, false));
        BOOST_CHECK_EQUAL(toString(clipSet), "");
    }

    {
        // Infinite (external) lower bound, intersecting upper bound
        // Clip: << 15) --> [1 7) (7 10) (13 15)
        IntervalSet<int> clipSet(testSet);
        clipSet.clip(makeUpperBound<int>(15, false));
        BOOST_CHECK_EQUAL(toString(clipSet), "[1 7) (7 10) (13 15)");
    }

    {
        // Internal lower bound, infinite (equal) upper bound
        // Clip: (15 >> --> (15 21] [22 >>
        IntervalSet<int> clipSet(testSet);
        clipSet.clip(makeLowerBound<int>(15, false));
        BOOST_CHECK_EQUAL(toString(clipSet), "(15 21] [22 >>");
    }

    {
        // Equal upper and lower bound
        // Clip: (7 10) --> (7 10)
        IntervalSet<int> clipSet(testSet);
        clipSet.clip(makeInterval<int>(7, 10, false, false));
        BOOST_CHECK_EQUAL(toString(clipSet), "(7 10)");
    }

    {
        // Infinite clip
        // Clip: << >> --> [1 7) (7 10) (13 21] [22 >>
        IntervalSet<int> clipSet(testSet);
        clipSet.clip(Interval<int>().setInfinite());
        BOOST_CHECK_EQUAL(toString(clipSet), "[1 7) (7 10) (13 21] [22 >>");
    }

    {
        // External clip region surrounding multiple intervals in set
        // Clip: [7 22) --> (7 10) (13 21]
        IntervalSet<int> clipSet(testSet);
        clipSet.clip(makeInterval<int>(7, 22, true, false));
        BOOST_CHECK_EQUAL(toString(clipSet), "(7 10) (13 21]");
    }
}
