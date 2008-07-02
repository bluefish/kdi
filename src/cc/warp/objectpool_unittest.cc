//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/warp/objectpool_unittest.cc#1 $
//
// Created 2007/03/23
//
// Copyright 2007 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#include "objectpool.h"
#include "unittest/main.h"

using namespace warp;
using namespace ex;

namespace
{
    struct Thing
    {
        static size_t nConstructed;
        
        Thing * me;

        Thing() : me(this)
        {
            ++nConstructed;
        }

        ~Thing()
        {
            BOOST_CHECK_EQUAL(me, this);
            --nConstructed;
        }
    };

    size_t Thing::nConstructed = 0;
}

BOOST_AUTO_TEST_CASE(reconstructing_pool)
{
    ObjectPool<Thing> * p1 = new ObjectPool<Thing>(4, true);
    ObjectPool<Thing> * p2 = new ObjectPool<Thing>(4, true);

    BOOST_CHECK_EQUAL(Thing::nConstructed, 0u);

    Thing * t1 = p1->get();
    BOOST_CHECK_EQUAL(Thing::nConstructed, 1u);
    BOOST_CHECK_EQUAL(t1->me, t1);

    Thing * t2 = p1->get();
    BOOST_CHECK_EQUAL(Thing::nConstructed, 2u);
    BOOST_CHECK_EQUAL(t2->me, t2);

    Thing * t3 = p2->get();
    BOOST_CHECK_EQUAL(Thing::nConstructed, 3u);
    BOOST_CHECK_EQUAL(t3->me, t3);

    p1->release(t1);
    BOOST_CHECK_EQUAL(Thing::nConstructed, 2u);

    Thing * t4 = p1->get();
    BOOST_CHECK_EQUAL(Thing::nConstructed, 3u);
    BOOST_CHECK_EQUAL(t4->me, t4);
    BOOST_CHECK_EQUAL(t1, t4);

    BOOST_CHECK_THROW(p1->release(t3), RuntimeError);
        
    delete p1;
    BOOST_CHECK_EQUAL(Thing::nConstructed, 1u);
        
    p2->release(t3);
    BOOST_CHECK_EQUAL(Thing::nConstructed, 0u);

    delete p2;
    BOOST_CHECK_EQUAL(Thing::nConstructed, 0u);
}

BOOST_AUTO_TEST_CASE(recycling_pool)
{
    ObjectPool<Thing> * p1 = new ObjectPool<Thing>(4, false);
    ObjectPool<Thing> * p2 = new ObjectPool<Thing>(4, false);

    BOOST_CHECK_EQUAL(Thing::nConstructed, 0u);

    Thing * t1 = p1->get();
    BOOST_CHECK_EQUAL(Thing::nConstructed, 1u);
    BOOST_CHECK_EQUAL(t1->me, t1);

    Thing * t2 = p1->get();
    BOOST_CHECK_EQUAL(Thing::nConstructed, 2u);
    BOOST_CHECK_EQUAL(t2->me, t2);

    Thing * t3 = p2->get();
    BOOST_CHECK_EQUAL(Thing::nConstructed, 3u);
    BOOST_CHECK_EQUAL(t3->me, t3);

    p1->release(t1);
    BOOST_CHECK_EQUAL(Thing::nConstructed, 3u);

    Thing * t4 = p1->get();
    BOOST_CHECK_EQUAL(Thing::nConstructed, 3u);
    BOOST_CHECK_EQUAL(t4->me, t4);
    BOOST_CHECK_EQUAL(t1, t4);

    BOOST_CHECK_THROW(p1->release(t3), RuntimeError);
        
    delete p1;
    BOOST_CHECK_EQUAL(Thing::nConstructed, 1u);
        
    p2->release(t3);
    BOOST_CHECK_EQUAL(Thing::nConstructed, 1u);

    delete p2;
    BOOST_CHECK_EQUAL(Thing::nConstructed, 0u);
}
