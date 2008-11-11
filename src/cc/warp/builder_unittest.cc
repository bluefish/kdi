//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: warp/builder_unittest.cc $
//
// Created 2008/11/11
//
// Copyright 2008 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#include <warp/builder.h>
#include <warp/offset.h>
#include <vector>
#include <unittest/main.h>

using namespace warp;

namespace {

    struct FooData
    {
        int32_t foo;
        int32_t bar;
        int16_t baz;
        int16_t quux;
    };


}

BOOST_AUTO_UNIT_TEST(build_basic)
{
    // Build a FooData
    Builder b;
    BOOST_CHECK(!b.isFinalized());
    BOOST_CHECK_EQUAL(b.size(), 0u);

    // Write foo = 42
    int32_t foo = 42;           
    b.write(0, &foo, 4);
    BOOST_CHECK(!b.isFinalized());
    BOOST_CHECK_EQUAL(b.size(), 4u);

    // Write bar = 77
    b.write<int32_t>(4, 77);
    BOOST_CHECK(!b.isFinalized());
    BOOST_CHECK_EQUAL(b.size(), 8u);

    // Write baz = -14
    int16_t baz = -14;
    b.append(&baz, 2);
    BOOST_CHECK(!b.isFinalized());
    BOOST_CHECK_EQUAL(b.size(), 10u);

    // Write quux = 11
    b.append<int16_t>(11);
    BOOST_CHECK(!b.isFinalized());
    BOOST_CHECK_EQUAL(b.size(), 12u);

    // Finalize
    b.finalize();
    BOOST_CHECK(b.isFinalized());
    BOOST_CHECK_EQUAL(b.size(), 12u);
    BOOST_CHECK_EQUAL(b.getFinalSize(), sizeof(FooData));

    // Export
    FooData d;
    b.exportTo(&d);
    BOOST_CHECK_EQUAL(d.foo, 42);
    BOOST_CHECK_EQUAL(d.bar, 77);
    BOOST_CHECK_EQUAL(d.baz, -14);
    BOOST_CHECK_EQUAL(d.quux, 11);
    
    // Resume
    b.resume();
    BOOST_CHECK(!b.isFinalized());
    BOOST_CHECK_EQUAL(b.size(), 12u);

    // Rewrite bar = 78
    int32_t bar = 78;
    b.write(4, bar);
    BOOST_CHECK(!b.isFinalized());
    BOOST_CHECK_EQUAL(b.size(), 12u);

    // Finalize again
    b.finalize();
    BOOST_CHECK(b.isFinalized());
    BOOST_CHECK_EQUAL(b.size(), 12u);
    BOOST_CHECK_EQUAL(b.getFinalSize(), sizeof(FooData));

    // Export again
    b.exportTo(&d);
    BOOST_CHECK_EQUAL(d.foo, 42);
    BOOST_CHECK_EQUAL(d.bar, 78);
    BOOST_CHECK_EQUAL(d.baz, -14);
    BOOST_CHECK_EQUAL(d.quux, 11);
    
    // Reset
    b.reset();
    BOOST_CHECK(!b.isFinalized());
    BOOST_CHECK_EQUAL(b.size(), 0u);

    // Rebuild
    foo = -3000;
    bar = 12345;
    b.append(foo);              // foo = -3000
    BOOST_CHECK_EQUAL(b.size(), 4u);

    b.write<int16_t>(8, 55);    // baz = 55
    BOOST_CHECK_EQUAL(b.size(), 10u);

    b.append<int16_t>(4);       // quux = 4
    BOOST_CHECK_EQUAL(b.size(), 12u);

    b.write(4, bar);            // bar = 12345
    BOOST_CHECK_EQUAL(b.size(), 12u);

    // Export
    b.finalize();
    b.exportTo(&d);
    BOOST_CHECK_EQUAL(d.foo, -3000);
    BOOST_CHECK_EQUAL(d.bar, 12345);
    BOOST_CHECK_EQUAL(d.baz, 55);
    BOOST_CHECK_EQUAL(d.quux, 4);
}

namespace {

    struct LinkedNode
    {
        int32_t num;
        Offset<LinkedNode> next;
    };

}

BOOST_AUTO_UNIT_TEST(build_reference)
{
    Builder b;

    // Get some subblocks.  It doesn't matter where it comes from,
    // it's all the same builder.
    BuilderBlock * node1 = &b;
    BuilderBlock * node2 = b.subblock(4);
    BuilderBlock * node34 = node2->subblock(4);

    // Build: 1 -> 4 -> 2 -> 3 -> null
    
    // Node 1
    node1->writeOffset(4, node34, 8); // n1.next = n4 (node34 + 8)
    node1->write<int32_t>(0, 1);      // n1.num = 1

    // Node 2
    node2->append<int32_t>(2);        // n2.num = 2
    node2->appendOffset(node34);      // n2.next = n3

    // Node 3 and 4 are in the same block.  Node 4 starts at offset 8.
    node34->append<int32_t>(3);       // n3.num = 3
    node34->appendOffset(0);          // n3.next = null
    node34->writeOffset(12, node2);   // n4.next = n2
    node34->write<int32_t>(8, 4);     // n4.num = 4

    // Build and check
    b.finalize();
    std::vector<char> buf(b.getFinalSize());
    b.exportTo(&buf[0]);
    LinkedNode * n = (LinkedNode *)&buf[0];

    BOOST_CHECK_EQUAL(n->num, 1);
    BOOST_CHECK_EQUAL(n->next->num, 4);
    BOOST_CHECK_EQUAL(n->next->next->num, 2);
    BOOST_CHECK_EQUAL(n->next->next->next->num, 3);
    BOOST_CHECK(!n->next->next->next->next);

    // Rebuild with circular reference, 1 -> 4 -> 2 -> 3 -> 1 -> ...
    b.resume();
    node34->writeOffset(4, node1);

    b.finalize();
    buf.resize(b.getFinalSize());
    b.exportTo(&buf[0]);
    n = (LinkedNode *)&buf[0];
    
    BOOST_CHECK_EQUAL(n->num, 1);
    BOOST_CHECK_EQUAL(n->next->num, 4);
    BOOST_CHECK_EQUAL(n->next->next->num, 2);
    BOOST_CHECK_EQUAL(n->next->next->next->num, 3);
    BOOST_CHECK_EQUAL(n->next->next->next->next->num, 1);
    BOOST_CHECK_EQUAL(n->next->next->next->next->next->num, 4);
}
