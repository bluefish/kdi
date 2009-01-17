//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-03-23
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

#include <warp/objectpool.h>
#include <unittest/main.h>

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

namespace {

    struct Sum
    {
        int nArgs;
        int sum;

        Sum() : nArgs(0), sum(0) {}
        Sum(int a) : nArgs(1), sum(a) {}
        Sum(int a, int b) : nArgs(2), sum(a+b) {}
        Sum(int a, int b, int c) : nArgs(3), sum(a+b+c) {}
    };

}

BOOST_AUTO_TEST_CASE(create_vs_get)
{
    // Trying to test that objects returned from get() may be reused
    // when destroyOnRelease=false, but objects from create() are
    // always reconstructed.

    // Pool size of 1, hoping to predict which object we get back
    ObjectPool<Sum> p(1, false);

    Sum * x = p.get();
    BOOST_CHECK_EQUAL(x->nArgs, 0);
    BOOST_CHECK_EQUAL(x->sum, 0);
    x->nArgs = -1;

    p.release(x);
    x = p.get();   // not actually guaranteed to get same object back
    BOOST_CHECK_EQUAL(x->nArgs, -1);
    BOOST_CHECK_EQUAL(x->sum, 0);

    p.release(x);
    x = p.create(3,4);
    BOOST_CHECK_EQUAL(x->nArgs, 2);
    BOOST_CHECK_EQUAL(x->sum, 7);

    p.release(x);
}

BOOST_AUTO_TEST_CASE(n_arg_create)
{
    ObjectPool<Sum> p(4);

    Sum * x = p.create();
    BOOST_CHECK_EQUAL(x->nArgs, 0);
    BOOST_CHECK_EQUAL(x->sum, 0);
    p.release(x);

    x = p.create(5);
    BOOST_CHECK_EQUAL(x->nArgs, 1);
    BOOST_CHECK_EQUAL(x->sum, 5);
    p.release(x);

    x = p.create(5,10);
    BOOST_CHECK_EQUAL(x->nArgs, 2);
    BOOST_CHECK_EQUAL(x->sum, 15);
    p.release(x);

    x = p.create(5,10,15);
    BOOST_CHECK_EQUAL(x->nArgs, 3);
    BOOST_CHECK_EQUAL(x->sum, 30);
    p.release(x);
}
