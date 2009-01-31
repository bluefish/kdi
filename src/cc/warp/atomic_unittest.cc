//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
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

#include <warp/atomic.h>
#include <unittest/main.h>
#include <boost/thread.hpp>
#include <boost/bind.hpp>

BOOST_AUTO_TEST_CASE(atomic_basic)
{
    warp::AtomicCounter c(3);

    BOOST_CHECK_EQUAL(c.get(), 3);
    c.increment();
    BOOST_CHECK_EQUAL(c.get(), 4);
    BOOST_CHECK_EQUAL(c.decrementAndTest(), false);
    BOOST_CHECK_EQUAL(c.decrementAndTest(), false);
    BOOST_CHECK_EQUAL(c.get(), 2);
    BOOST_CHECK_EQUAL(c.decrementAndTest(), false);
    BOOST_CHECK_EQUAL(c.decrementAndTest(), true);
    BOOST_CHECK_EQUAL(c.get(), 0);
}

void upDown(size_t step, size_t total, warp::AtomicCounter * counter)
{
    for(size_t i = 0; i < total; i += step)
    {
        for(size_t j = 0; j < step; ++j)
            counter->increment();

        for(size_t j = 0; j < step; ++j)
            counter->decrementAndTest();
    }
}

BOOST_AUTO_TEST_CASE(atomic_mt)
{
    warp::AtomicCounter c(7);
    size_t const TOTAL = 10000000;

    boost::thread_group threads;
    threads.create_thread(boost::bind(&upDown,     1, TOTAL, &c));
    threads.create_thread(boost::bind(&upDown,     2, TOTAL, &c));
    threads.create_thread(boost::bind(&upDown,    25, TOTAL, &c));
    threads.create_thread(boost::bind(&upDown,   277, TOTAL, &c));
    threads.create_thread(boost::bind(&upDown,  1000, TOTAL, &c));
    threads.create_thread(boost::bind(&upDown, TOTAL, TOTAL, &c));
    threads.join_all();

    BOOST_CHECK_EQUAL(c.get(), 7);
}
