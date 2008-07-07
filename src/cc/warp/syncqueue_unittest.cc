//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-03-26
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

#include <warp/syncqueue.h>
#include <unittest/main.h>
#include <boost/bind.hpp>
#include <boost/thread.hpp>
#include <iostream>

using namespace warp;
using namespace boost;
using namespace std;

namespace
{
    void pusher(SyncQueue<size_t> & q, size_t count)
    {
        for(size_t i = 0; i < count; ++i)
        {
            ; // cout << "push " << i << endl;
            if(q.push(i, true))
                ; // cout << "pushed " << i << endl;
            else
                ; // cout << "didn't push " << i << endl;
        }
    }

    void pusher_timed(SyncQueue<size_t> & q, double timeout, size_t count)
    {
        for(size_t i = 0; i < count; ++i)
        {
            ; // cout << "push " << i << endl;
            if(q.push(i, timeout))
                ; // cout << "pushed " << i << endl;
            else
                ; // cout << "didn't push " << i << endl;
        }
    }

    void popper(SyncQueue<size_t> & q, bool verify, bool wait, size_t & count)
    {
        size_t n = 0;
        size_t got(-1);
        for(;;)
        {
            ; // cout << "popping" << endl;
            if(!q.pop(got, wait))
            {
                ; // cout << "didn't pop" << endl;
                break;
            }
            ; // cout << "popped " << got << endl;

            if(verify)
                BOOST_CHECK_EQUAL(got, n);
            ++n;

            q.taskComplete();
        }
        count = n;
    }

    void popper_timed(SyncQueue<size_t> & q, bool verify, double timeout, size_t & count)
    {
        size_t n = 0;
        size_t got(-1);
        for(;;)
        {
            ; // cout << "popping" << endl;
            if(!q.pop(got, timeout))
            {
                ; // cout << "didn't pop" << endl;
                break;
            }
            ; // cout << "popped " << got << endl;

            if(verify)
                BOOST_CHECK_EQUAL(got, n);
            ++n;

            q.taskComplete();
        }
        count = n;
    }
}

BOOST_AUTO_TEST_CASE(sync_test)
{
    SyncQueue<size_t> q;

    size_t inCount = 1000;
    size_t outCount;

    // Fill queue
    pusher(q, inCount);

    // Empty queue, verifying ordering
    popper(q, true, false, outCount);

    // Make sure we got as many items out as we put in
    BOOST_CHECK_EQUAL(inCount, outCount);
}

BOOST_AUTO_TEST_CASE(async_test)
{
    SyncQueue<size_t> q;

    size_t inCount = 100000;
    size_t outCount;

    thread consumer(bind(popper, ref(q), true, true, ref(outCount)));
    thread producer(bind(pusher, ref(q), inCount));

    producer.join();
    q.cancelWaits();
    consumer.join();

    // Make sure we got as many items out as we put in
    BOOST_CHECK_EQUAL(inCount, outCount);
}

BOOST_AUTO_TEST_CASE(async_bounded_test)
{
    SyncQueue<size_t> q(1000);

    size_t inCount = 100000;
    size_t outCount;

    thread consumer(bind(popper, ref(q), true, true, ref(outCount)));
    thread producer(bind(pusher, ref(q), inCount));

    producer.join();
    q.cancelWaits();
    consumer.join();

    // Make sure we got as many items out as we put in
    BOOST_CHECK_EQUAL(inCount, outCount);
}

BOOST_AUTO_TEST_CASE(async_timeout_test)
{
    SyncQueue<size_t> q;

    size_t inCount = 100000;
    size_t outCount;

    thread consumer(bind(popper_timed, ref(q), true, 0.25, ref(outCount)));
    thread producer(bind(pusher_timed, ref(q), 0.25, inCount));

    producer.join();
    consumer.join();

    // Make sure we got as many items out as we put in
    BOOST_CHECK_EQUAL(inCount, outCount);
}

BOOST_AUTO_TEST_CASE(async_multi_test)
{
    SyncQueue<size_t> q;

    size_t in1 = 100000;
    size_t in2 = 100000;
    size_t in3 = 100000;
    size_t out1;
    size_t out2;
    size_t out3;

    thread consumer1(bind(popper, ref(q), false, true, ref(out1)));
    thread consumer2(bind(popper, ref(q), false, true, ref(out2)));
    thread consumer3(bind(popper, ref(q), false, true, ref(out3)));
    thread producer1(bind(pusher, ref(q), in1));
    thread producer2(bind(pusher, ref(q), in2));
    thread producer3(bind(pusher, ref(q), in3));

    producer1.join();
    producer2.join();
    producer3.join();

    q.cancelWaits();
    consumer1.join();
    consumer2.join();
    consumer3.join();

    // Make sure we got as many items out as we put in
    BOOST_CHECK_EQUAL(in1 + in2 + in3, out1 + out2 + out3);
}

BOOST_AUTO_TEST_CASE(async_bounded_multi_test)
{
    SyncQueue<size_t> q(1000);

    size_t in1 = 100000;
    size_t in2 = 100000;
    size_t in3 = 100000;
    size_t out1;
    size_t out2;
    size_t out3;

    thread consumer1(bind(popper, ref(q), false, true, ref(out1)));
    thread consumer2(bind(popper, ref(q), false, true, ref(out2)));
    thread consumer3(bind(popper, ref(q), false, true, ref(out3)));
    thread producer1(bind(pusher, ref(q), in1));
    thread producer2(bind(pusher, ref(q), in2));
    thread producer3(bind(pusher, ref(q), in3));

    producer1.join();
    producer2.join();
    producer3.join();

    q.cancelWaits();
    consumer1.join();
    consumer2.join();
    consumer3.join();

    // Make sure we got as many items out as we put in
    BOOST_CHECK_EQUAL(in1 + in2 + in3, out1 + out2 + out3);
}

BOOST_AUTO_UNIT_TEST(wait_test)
{
    // Bounded queue of 3 items
    SyncQueue<int> q(3);

    // The queue is empty and has no pending items.  These calls
    // should succeed immediately.
    BOOST_CHECK_EQUAL(q.waitForEmpty(), true);
    BOOST_CHECK_EQUAL(q.waitForCompletion(), true);

    // Cancel waits.
    q.cancelWaits();

    // The wait calls should still succeed.  The conditions are still
    // true.
    BOOST_CHECK_EQUAL(q.waitForEmpty(), true);
    BOOST_CHECK_EQUAL(q.waitForCompletion(), true);

    // Push some items.  They're blocking pushes, but waits have been
    // cancelled.  They should succeed anyway.
    BOOST_CHECK_EQUAL(q.push(1), true);
    BOOST_CHECK_EQUAL(q.push(2), true);
    BOOST_CHECK_EQUAL(q.push(3), true);

    // Another push will hit the queue max size.  It won't block
    // because waits have been cancelled.
    BOOST_CHECK_EQUAL(q.push(4), false);

    // These conditions on these wait calls have changed.  The queue
    // isn't empty and nothing has been acknowledged.  Waits have been
    // cancelled, so these should return instead of block.
    BOOST_CHECK_EQUAL(q.waitForEmpty(), false);
    BOOST_CHECK_EQUAL(q.waitForCompletion(), false);

    // Pop and acknowledge some items.
    int i;
    BOOST_CHECK_EQUAL(q.pop(i), true);
    BOOST_CHECK_EQUAL(i, 1);
    BOOST_CHECK_EQUAL(q.pop(i), true);
    BOOST_CHECK_EQUAL(i, 2);
    q.taskComplete();
    q.taskComplete();

    // Still one item left
    BOOST_CHECK_EQUAL(q.waitForEmpty(), false);
    BOOST_CHECK_EQUAL(q.waitForCompletion(), false);
    
    // Get the last item
    BOOST_CHECK_EQUAL(q.pop(i), true);
    BOOST_CHECK_EQUAL(i, 3);

    // The queue is now empty, but there's still a pending
    // acknowledgement.
    BOOST_CHECK_EQUAL(q.waitForEmpty(), true);
    BOOST_CHECK_EQUAL(q.waitForCompletion(), false);

    // Acknowledge the last item.
    q.taskComplete();

    // We should be good again
    BOOST_CHECK_EQUAL(q.waitForEmpty(), true);
    BOOST_CHECK_EQUAL(q.waitForCompletion(), true);
    
    // Get some more -- should return because waits are cancelled.
    BOOST_CHECK_EQUAL(q.pop(i), false);
}
