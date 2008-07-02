//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: warp/circular_unittest.cc $
//
// Created 2008/06/11
//
// Copyright 2008 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#include <warp/circular.h>
#include <unittest/main.h>

using namespace warp;

namespace
{
    struct Num : public Circular<Num>
    {
        int val;

        Num(int val) : val(val) {}
    };
};

BOOST_AUTO_UNIT_TEST(circular_test)
{
    Num a(1);
    BOOST_CHECK(a.isSingular());
    BOOST_CHECK_EQUAL(a.next(), &a);
    BOOST_CHECK_EQUAL(a.prev(), &a);

    a.setSuccessor(&a);
    BOOST_CHECK(a.isSingular());
    BOOST_CHECK_EQUAL(a.next(), &a);
    BOOST_CHECK_EQUAL(a.prev(), &a);

    a.setPredecessor(&a);
    BOOST_CHECK(a.isSingular());
    BOOST_CHECK_EQUAL(a.next(), &a);
    BOOST_CHECK_EQUAL(a.prev(), &a);

    a.unlink();
    BOOST_CHECK(a.isSingular());
    BOOST_CHECK_EQUAL(a.next(), &a);
    BOOST_CHECK_EQUAL(a.prev(), &a);

    Num b(2);
    a.setSuccessor(&b);
    BOOST_CHECK(!a.isSingular());
    BOOST_CHECK(!b.isSingular());
    BOOST_CHECK_EQUAL(a.next(), &b);
    BOOST_CHECK_EQUAL(a.prev(), &b);
    BOOST_CHECK_EQUAL(b.next(), &a);
    BOOST_CHECK_EQUAL(b.prev(), &a);
    
    Num c(3);
    a.setPredecessor(&c);
    BOOST_CHECK_EQUAL(a.next(), &b);
    BOOST_CHECK_EQUAL(b.next(), &c);
    BOOST_CHECK_EQUAL(c.next(), &a);
    BOOST_CHECK_EQUAL(a.prev(), &c);
    BOOST_CHECK_EQUAL(b.prev(), &a);
    BOOST_CHECK_EQUAL(c.prev(), &b);

    a.unlink();
    BOOST_CHECK_EQUAL(a.next(), &a);
    BOOST_CHECK_EQUAL(a.prev(), &a);
    BOOST_CHECK_EQUAL(b.next(), &c);
    BOOST_CHECK_EQUAL(c.next(), &b);
    BOOST_CHECK_EQUAL(b.prev(), &c);
    BOOST_CHECK_EQUAL(c.prev(), &b);

    b.setPredecessor(&a);
    {
        Num d(4);
        Num e(5);
        d.setSuccessor(&e);
        BOOST_CHECK_EQUAL(d.next(), &e);
        BOOST_CHECK_EQUAL(e.next(), &d);

        c.setSuccessor(&d);
        BOOST_CHECK_EQUAL(a.next(), &b);
        BOOST_CHECK_EQUAL(b.next(), &c);
        BOOST_CHECK_EQUAL(c.next(), &d);
        BOOST_CHECK_EQUAL(d.next(), &a);
        BOOST_CHECK_EQUAL(e.next(), &e);

        d.setSuccessor(&e);
        BOOST_CHECK_EQUAL(a.next(), &b);
        BOOST_CHECK_EQUAL(b.next(), &c);
        BOOST_CHECK_EQUAL(c.next(), &d);
        BOOST_CHECK_EQUAL(d.next(), &e);
        BOOST_CHECK_EQUAL(e.next(), &a);
    }

    BOOST_CHECK_EQUAL(a.next(), &b);
    BOOST_CHECK_EQUAL(b.next(), &c);
    BOOST_CHECK_EQUAL(c.next(), &a);
}

BOOST_AUTO_UNIT_TEST(circular_list_test)
{
    CircularList<Num> l;

    BOOST_CHECK(l.empty());
    BOOST_CHECK(l.begin() == l.end());

    Num a(1);
    Num b(2);
    Num c(3);

    l.moveToFront(&a);
    BOOST_CHECK(!l.empty());

    {
        CircularList<Num> const & cl(l);
        CircularList<Num>::const_iterator i = cl.begin();
        BOOST_CHECK_EQUAL(i->val, 1);
        BOOST_CHECK(++i == cl.end());
        BOOST_CHECK_EQUAL(cl.front().val, 1);
        BOOST_CHECK_EQUAL(cl.back().val, 1);
    }

    l.moveToBack(&b);
    {
        CircularList<Num> const & cl(l);
        CircularList<Num>::const_iterator i = cl.begin();
        BOOST_CHECK_EQUAL(i->val, 1);
        BOOST_CHECK(++i != cl.end());
        BOOST_CHECK_EQUAL(i->val, 2);
        BOOST_CHECK(++i == cl.end());
        BOOST_CHECK_EQUAL(cl.front().val, 1);
        BOOST_CHECK_EQUAL(cl.back().val, 2);
    }

    l.moveToBack(&c);
    {
        CircularList<Num>::iterator i = l.begin();
        BOOST_CHECK_EQUAL(i->val, 1);
        BOOST_CHECK(++i != l.end());
        BOOST_CHECK_EQUAL(i->val, 2);
        BOOST_CHECK(++i != l.end());
        BOOST_CHECK_EQUAL(i->val, 3);
        BOOST_CHECK(++i == l.end());
        BOOST_CHECK_EQUAL(l.front().val, 1);
        BOOST_CHECK_EQUAL(l.back().val, 3);
    }

    l.moveToFront(&b);
    {
        CircularList<Num>::iterator i = l.begin();
        BOOST_CHECK_EQUAL(i->val, 2);
        BOOST_CHECK(++i != l.end());
        BOOST_CHECK_EQUAL(i->val, 1);
        BOOST_CHECK(++i != l.end());
        BOOST_CHECK_EQUAL(i->val, 3);
        BOOST_CHECK(++i == l.end());
        BOOST_CHECK_EQUAL(l.front().val, 2);
        BOOST_CHECK_EQUAL(l.back().val, 3);
    }

    b.unlink();
    {
        CircularList<Num> const & cl(l);
        CircularList<Num>::const_iterator i = cl.begin();
        BOOST_CHECK_EQUAL(i->val, 1);
        BOOST_CHECK(++i != cl.end());
        BOOST_CHECK_EQUAL(i->val, 3);
        BOOST_CHECK(++i == cl.end());
        BOOST_CHECK_EQUAL(cl.front().val, 1);
        BOOST_CHECK_EQUAL(cl.back().val, 3);
    }
}
