//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-09-18
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

#include <flux/cutoff.h>
#include <flux/sequence.h>
#include <unittest/main.h>
#include <deque>

using namespace flux;
using namespace std;

BOOST_AUTO_UNIT_TEST(cutoff_input_stream)
{
    // input sequences and cutoffs
    int const A[] = { 1, 4, 6, 6, 7, 12, 15 };
    int const cutoff1 = 6;
    int const cutoff2 = 12;
    
    // output sequences
    int const firstChunk[] = { 1, 4 }; 
    int const secondChunk[] = { 6, 6, 7 };
    int const rest[] = { 12, 15 };

    Stream<int>::handle_t a(new Sequence< deque<int> >(deque<int>(A, A+sizeof(A)/sizeof(*A))));
    Cutoff<int>::handle_t cutter = makeCutoff<int>();
    cutter->pipeFrom(a);

    int x;

    cutter->setCutoff(cutoff1);
    for(size_t i = 0; i < sizeof(firstChunk)/sizeof(*firstChunk); ++i)
    {
        BOOST_CHECK(cutter->get(x));
        BOOST_CHECK_EQUAL(x, firstChunk[i]);
    }
    BOOST_CHECK(!cutter->get(x));

    cutter->setCutoff(cutoff2);
    for(size_t i = 0; i < sizeof(secondChunk)/sizeof(*secondChunk); ++i)
    {
        BOOST_CHECK(cutter->get(x));
        BOOST_CHECK_EQUAL(x, secondChunk[i]);
    }
    BOOST_CHECK(!cutter->get(x));

    cutter->clearCutoff();
    for(size_t i = 0; i < sizeof(rest)/sizeof(*rest); ++i)
    {
        BOOST_CHECK(cutter->get(x));
        BOOST_CHECK_EQUAL(x, rest[i]);
    }
    BOOST_CHECK(!cutter->get(x));
}
