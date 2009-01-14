//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-01-10
//
// This file is part of the flux library.
//
// The flux library is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by the
// Free Software Foundation; either version 2 of the License, or any later
// version.
//
// The flux library is distributed in the hope that it will be useful, but
// WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General
// Public License for more details.
//
// You should have received a copy of the GNU General Public License along
// with this program; if not, write to the Free Software Foundation, Inc.,
// 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.
//----------------------------------------------------------------------------

#include <flux/threaded_reader.h>
#include <flux/sequence.h>
#include <unittest/main.h>
#include <deque>

using namespace flux;
using namespace std;

BOOST_AUTO_UNIT_TEST(int_reader)
{
    // Sequence of numbers
    int const S[] = { 1, 1, 1, 2, 2, 3, 4, 5, 6, 6, 7, 7, 7, 9, 11, 12, 12,
                      12, 13, 14, 15 };
    Stream<int>::handle_t s(new Sequence< deque<int> >(deque<int>(S, S+sizeof(S)/sizeof(*S))));

    // Worker pool -- 3 worker threads
    warp::WorkerPool pool(3, "TestPool", false);

    // Threaded reader -- read ahead 4
    Stream<int>::handle_t r = makeThreadedReader<int>(pool, 4);
    r->pipeFrom(s);

    int x;
    for(size_t i = 0; i < sizeof(S)/sizeof(*S); ++i)
    {
        BOOST_CHECK(r->get(x));
        BOOST_CHECK_EQUAL(x, S[i]);
    }
    BOOST_CHECK(!r->get(x));
}
