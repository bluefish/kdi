//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: warp/synchronized_unittest.cc $
//
// Created 2008/04/09
//
// Copyright 2008 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#include <warp/synchronized.h>
#include <unittest/main.h>
#include <string>
#include <map>

using namespace warp;
using namespace std;

BOOST_AUTO_UNIT_TEST(basic)
{
    // These tests aren't checking multi-threaded safety.  It's
    // assumed the boost::mutex stuff works.  This is just checking
    // that the templates actually compile as expected.

    // Try a primitive type
    {
        Synchronized<int> x(8);

        LockedPtr<int> lx(x);
        BOOST_CHECK_EQUAL(*lx, 8);
        (*lx)++;
        BOOST_CHECK_EQUAL(*lx, 9);
    }

    // Try a map
    {
        typedef map<string,string> ssmap;
        Synchronized<ssmap> syncMap;

        LockedPtr<ssmap> m(syncMap);
        
        m->insert(make_pair("foo","bar"));
        (*m)["llama"] = "dingo";

        BOOST_CHECK_EQUAL(m->find("llama")->second, "dingo");
        BOOST_CHECK_EQUAL(m->size(), 2u);
    }
}


namespace
{
    struct A
    {
        int a;
        A() : a(0) {}
    };
    
    struct B : public A
    {
        int b;
        B() : A(), b(0) {}
    };
}

BOOST_AUTO_UNIT_TEST(type_conversion_test)
{
    Synchronized<A> sA;
    Synchronized<B> sB;

    Synchronized<A> const csA;
    Synchronized<B> const csB;

    Synchronized<A const> scA;
    Synchronized<B const> scB;

    Synchronized<A const> const cscA;
    Synchronized<B const> const cscB;

    { LockedPtr<A> p(sA); }              // ok:   A&       -> A&
    { LockedPtr<A> p(sB); }              // ok:   B&       -> A&
    //{ LockedPtr<A> p(csA); }           // fail: const A& -> A&
    //{ LockedPtr<A> p(csB); }           // fail: const B& -> A&
    //{ LockedPtr<A> p(scA); }           // fail: const A& -> A&
    //{ LockedPtr<A> p(scB); }           // fail: const B& -> A&
    //{ LockedPtr<A> p(cscA); }          // fail: const A& -> A&
    //{ LockedPtr<A> p(cscB); }          // fail: const B& -> A&

    //{ LockedPtr<B> p(sA); }            // fail: A&       -> B&
    { LockedPtr<B> p(sB); }              // ok:   B&       -> B&
    //{ LockedPtr<B> p(csA); }           // fail: const A& -> B&
    //{ LockedPtr<B> p(csB); }           // fail: const B& -> B&
    //{ LockedPtr<B> p(scA); }           // fail: const A& -> B&
    //{ LockedPtr<B> p(scB); }           // fail: const B& -> B&
    //{ LockedPtr<B> p(cscA); }          // fail: const A& -> B&
    //{ LockedPtr<B> p(cscB); }          // fail: const B& -> B&

    { LockedPtr<A const> p(sA); }        // ok:   A&       -> const A&
    { LockedPtr<A const> p(sB); }        // ok:   B&       -> const A&
    { LockedPtr<A const> p(csA); }       // ok:   const A& -> const A&
    { LockedPtr<A const> p(csB); }       // ok:   const B& -> const A&
    { LockedPtr<A const> p(scA); }       // ok:   const A& -> const A&
    { LockedPtr<A const> p(scB); }       // ok:   const B& -> const A&
    { LockedPtr<A const> p(cscA); }      // ok:   const A& -> const A&
    { LockedPtr<A const> p(cscB); }      // ok:   const B& -> const A&

    //{ LockedPtr<B const> p(sA); }      // fail: A&       -> const B&
    { LockedPtr<B const> p(sB); }        // ok:   B&       -> const B&
    //{ LockedPtr<B const> p(csA); }     // fail: const A& -> const B&
    { LockedPtr<B const> p(csB); }       // ok:   const B& -> const B&
    //{ LockedPtr<B const> p(scA); }     // fail: const A& -> const B&
    { LockedPtr<B const> p(scB); }       // ok:   const B& -> const B&
    //{ LockedPtr<B const> p(cscA); }    // fail: const A& -> const B&
    { LockedPtr<B const> p(cscB); }      // ok:   const B& -> const B&
}
