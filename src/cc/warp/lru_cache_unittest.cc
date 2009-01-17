//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
// Created 2008-06-11
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

#include <warp/lru_cache.h>
#include <unittest/main.h>

using namespace warp;

namespace
{
    int nFoo = 0;

    struct Foo
    {
        int value;
        Foo() : value(-1) { ++nFoo; }
        ~Foo() { value = -2; --nFoo; }
    };
}

BOOST_AUTO_UNIT_TEST(lru_basic)
{
    LruCache<std::string, Foo> lru(3);

    // Cache: EMPTY
    BOOST_CHECK_EQUAL(nFoo, 0);

    {
        // Insert one item -- should be a new item
        Foo * a = lru.get("42");
        BOOST_CHECK_EQUAL(a->value, -1);
        BOOST_CHECK_EQUAL(nFoo, 1);

        // Cache: "42"(1)
        BOOST_CHECK_EQUAL(nFoo, 1);

        // Set a value
        a->value = 42;

        // Get the same item
        Foo * b = lru.get("42");
        BOOST_CHECK_EQUAL(a, b);
        BOOST_CHECK_EQUAL(b->value, 42);

        // Cache: "42"(2)
        BOOST_CHECK_EQUAL(nFoo, 1);

        // Release both items
        lru.release(a);
        lru.release(b);

        // Cache: "42"(0)
        BOOST_CHECK_EQUAL(nFoo, 1);
    }

    // Cache: "42"(0)
    BOOST_CHECK_EQUAL(nFoo, 1);

    // Get the item again and hold it
    Foo * held = lru.get("42");
    BOOST_CHECK_EQUAL(held->value, 42);

    // Cache: "42"(1)
    BOOST_CHECK_EQUAL(nFoo, 1);

    // Get a new item, set a value, and release it
    {
        Foo * x = lru.get("4");
        BOOST_CHECK_EQUAL(x->value, -1);
        x->value = 4;
        lru.release(x);

        // Cache: "4"(0) "42"(1)
        BOOST_CHECK_EQUAL(nFoo, 2);
    }

    // Force cache to drop something
    {
        // Cache: "4"(0) "42"(1)
        BOOST_CHECK_EQUAL(nFoo, 2);

        Foo * x = lru.get("77");
        BOOST_CHECK_EQUAL(x->value, -1);
        x->value = 77;
        lru.release(x);

        // Cache: "77"(0) "4"(0) "42"(1)
        BOOST_CHECK_EQUAL(nFoo, 3);

        x = lru.get("81");
        BOOST_CHECK_EQUAL(x->value, -1);
        x->value = 81;
        lru.release(x);

        // Cache: "81"(0) "77"(0) "42"(1)
        BOOST_CHECK_EQUAL(nFoo, 3);

        x = lru.get("77");
        BOOST_CHECK_EQUAL(x->value, 77);
        lru.release(x);

        // Cache: "77"(0) "81"(0) "42"(1)
        BOOST_CHECK_EQUAL(nFoo, 3);

        x = lru.get("4");
        BOOST_CHECK_EQUAL(x->value, -1);
        x->value = 4;
        lru.release(x);

        // Cache: "4"(0) "77"(0) "42"(1)
        BOOST_CHECK_EQUAL(nFoo, 3);

        // Release the held reference
        lru.release(held);

        // Cache: "4"(0) "77"(0) "42"(0)
        BOOST_CHECK_EQUAL(nFoo, 3);

        x = lru.get("42");
        BOOST_CHECK_EQUAL(x->value, 42);
        lru.release(x);

        // Cache: "42"(0) "4"(0) "77"(0)
        BOOST_CHECK_EQUAL(nFoo, 3);
    }

    // Force cache to go over budget
    {
        // Cache: "42"(0) "4"(0) "77"(0)
        BOOST_CHECK_EQUAL(nFoo, 3);

        Foo * x = lru.get("42");
        BOOST_CHECK_EQUAL(x->value, 42);

        // Cache: "42"(1) "4"(0) "77"(0)
        BOOST_CHECK_EQUAL(nFoo, 3);

        Foo * y = lru.get("77");
        BOOST_CHECK_EQUAL(y->value, 77);

        // Cache: "77"(1) "42"(1) "4"(0)
        BOOST_CHECK_EQUAL(nFoo, 3);

        Foo * z = lru.get("23");
        BOOST_CHECK_EQUAL(z->value, -1);
        z->value = 23;

        // Cache: "23"(1) "77"(1) "42"(1)
        BOOST_CHECK_EQUAL(nFoo, 3);

        Foo * w = lru.get("4");
        BOOST_CHECK_EQUAL(w->value, -1);
        w->value = 4;

        // Cache: "4"(1) "23"(1) "77"(1) "42"(1)
        BOOST_CHECK_EQUAL(nFoo, 4);

        BOOST_CHECK_EQUAL(x->value, 42);
        BOOST_CHECK_EQUAL(y->value, 77);
        BOOST_CHECK_EQUAL(z->value, 23);
        BOOST_CHECK_EQUAL(w->value, 4);

        Foo * xx = lru.get("42");
        BOOST_CHECK_EQUAL(xx->value, 42);

        // Cache: "42"(2) "4"(1) "23"(1) "77"(1)
        BOOST_CHECK_EQUAL(nFoo, 4);

        // Releasing an item when cache is over budget should drop it immediately
        lru.release(w);

        // Cache: "42"(2) "23"(1) "77"(1)
        BOOST_CHECK_EQUAL(nFoo, 3);

        // Release an item with immediate removal flag
        lru.release(z, true);

        // Cache: "42"(2) "77"(1)
        BOOST_CHECK_EQUAL(nFoo, 2);

        // Release an item with removal flag and an oustanding
        // reference.  Should remove until last reference has gone.
        lru.release(x, true);

        // Cache: "42"(1)* "77"(1)
        BOOST_CHECK_EQUAL(nFoo, 2);
        BOOST_CHECK_EQUAL(xx->value, 42);

        // Release other reference, no removal flag.  The old remove
        // request should carry.
        lru.release(xx);

        // Cache: "77"(1)
        BOOST_CHECK_EQUAL(nFoo, 1);

        // Remove item by key -- shouldn't happen until reference is
        // released.
        lru.remove("77");

        // Cache: "77"(1)*
        BOOST_CHECK_EQUAL(nFoo, 1);
        BOOST_CHECK_EQUAL(y->value, 77);

        // Releasing last reference should remove item
        lru.release(y);

        // Cache: EMPTY
        BOOST_CHECK_EQUAL(nFoo, 0);

        // Removing a non-existant key should do nothing
        lru.remove("42");

        // Cache: EMPTY
        BOOST_CHECK_EQUAL(nFoo, 0);

    }

    {
        // Add an item and remove after there are no references to it...

        // Cache: EMPTY
        BOOST_CHECK_EQUAL(nFoo, 0);

        // Add, set, and release
        Foo * x = lru.get("33");
        BOOST_CHECK_EQUAL(x->value, -1);
        x->value = 33;
        lru.release(x);

        // Cache: "33"(0)
        BOOST_CHECK_EQUAL(nFoo, 1);

        // Remove key
        lru.remove("33");

        // Cache: EMPTY
        BOOST_CHECK_EQUAL(nFoo, 0);
    }
}


namespace {

    struct SizeIsValue
    {
        size_t operator()(size_t x) const { return x; }
    };

    struct InitAsDouble
    {
        void operator()(size_t & val, size_t key) const
        {
            val = key * 2;
        }
    };

}

BOOST_AUTO_UNIT_TEST(lru_custom)
{
    // Test custom initialization and size:
    LruCache<size_t, size_t, InitAsDouble, SizeIsValue> cache(100);

    BOOST_CHECK_EQUAL(cache.size(), 0u);
    BOOST_CHECK_EQUAL(cache.count(), 0u);

    size_t * x = cache.get(10u);
    BOOST_CHECK_EQUAL(*x, 20u);
    BOOST_CHECK_EQUAL(cache.size(), 20u);
    BOOST_CHECK_EQUAL(cache.count(), 1u);

    size_t * y = cache.get(30u);
    BOOST_CHECK_EQUAL(*y, 60u);
    BOOST_CHECK_EQUAL(cache.size(), 80u);
    BOOST_CHECK_EQUAL(cache.count(), 2u);

    cache.release(x);
    BOOST_CHECK_EQUAL(cache.size(), 80u);
    BOOST_CHECK_EQUAL(cache.count(), 2u);

    x = cache.get(10u);
    BOOST_CHECK_EQUAL(*x, 20u);
    BOOST_CHECK_EQUAL(cache.size(), 80u);
    BOOST_CHECK_EQUAL(cache.count(), 2u);

    cache.release(y);
    BOOST_CHECK_EQUAL(cache.size(), 80u);
    BOOST_CHECK_EQUAL(cache.count(), 2u);

    size_t * z = cache.get(50u); // replace y
    BOOST_CHECK_EQUAL(*z, 100u);
    BOOST_CHECK_EQUAL(cache.size(), 120u);
    BOOST_CHECK_EQUAL(cache.count(), 2u);

    cache.release(x);           // remove x
    BOOST_CHECK_EQUAL(cache.size(), 100u);
    BOOST_CHECK_EQUAL(cache.count(), 1u);

    cache.release(z);
    BOOST_CHECK_EQUAL(cache.size(), 100u);
    BOOST_CHECK_EQUAL(cache.count(), 1u);

    x = cache.get(15u);         // remove z
    BOOST_CHECK_EQUAL(*x, 30u);
    BOOST_CHECK_EQUAL(cache.size(), 30u);
    BOOST_CHECK_EQUAL(cache.count(), 1u);
}
