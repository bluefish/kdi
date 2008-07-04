//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-09-18
//
// This file is part of KDI.
// 
// KDI is free software; you can redistribute it and/or modify it
// under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// any later version.
// 
// KDI is distributed in the hope that it will be useful, but WITHOUT
// ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
// or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public
// License for more details.
// 
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
// 02110-1301, USA.
//----------------------------------------------------------------------------

#include <flux/merge.h>
#include <flux/sequence.h>
#include <unittest/main.h>
#include <deque>

using namespace flux;
using namespace std;

BOOST_AUTO_UNIT_TEST(merge_int)
{
    // Three input sequences
    int const A[] = { 1, 4, 6, 6, 7, 12, 15 };
    int const B[] = { 2, 2, 3, 7, 11, 12, 12 };
    int const C[] = { 1, 1, 5, 7, 9, 13, 14 };
    
    // Merged sequence
    int const M[] = { 1, 1, 1, 2, 2, 3, 4, 5, 6, 6, 7, 7, 7, 9, 11, 12, 12,
                      12, 13, 14, 15 };

    Stream<int>::handle_t a(new Sequence< deque<int> >(deque<int>(A, A+sizeof(A)/sizeof(*A))));
    Stream<int>::handle_t b(new Sequence< deque<int> >(deque<int>(B, B+sizeof(B)/sizeof(*B))));
    Stream<int>::handle_t c(new Sequence< deque<int> >(deque<int>(C, C+sizeof(C)/sizeof(*C))));
    
    Stream<int>::handle_t m = makeMerge<int>();
    m->pipeFrom(a);
    m->pipeFrom(b);
    m->pipeFrom(c);

    int x;
    for(size_t i = 0; i < sizeof(M)/sizeof(*M); ++i)
    {
        BOOST_CHECK(m->get(x));
        BOOST_CHECK_EQUAL(x, M[i]);
    }
    BOOST_CHECK(!m->get(x));
}

BOOST_AUTO_UNIT_TEST(merge_unique_int)
{
    // Three input sequences
    int const A[] = { 1, 4, 6, 6, 7, 12, 15 };
    int const B[] = { 2, 2, 3, 7, 11, 12, 12 };
    int const C[] = { 1, 1, 5, 7, 9, 13, 14 };

    // Merge-uniqued sequence
    int const M[] = { 1, 2, 3, 4, 5, 6, 7, 9, 11, 12, 13, 14, 15 };

    Stream<int>::handle_t a(new Sequence< deque<int> >(deque<int>(A, A+sizeof(A)/sizeof(*A))));
    Stream<int>::handle_t b(new Sequence< deque<int> >(deque<int>(B, B+sizeof(B)/sizeof(*B))));
    Stream<int>::handle_t c(new Sequence< deque<int> >(deque<int>(C, C+sizeof(C)/sizeof(*C))));
    
    Stream<int>::handle_t m = makeMergeUnique<int>();
    m->pipeFrom(a);
    m->pipeFrom(b);
    m->pipeFrom(c);

    int x;
    for(size_t i = 0; i < sizeof(M)/sizeof(*M); ++i)
    {
        BOOST_CHECK(m->get(x));
        BOOST_CHECK_EQUAL(x, M[i]);
    }
    BOOST_CHECK(!m->get(x));
}
