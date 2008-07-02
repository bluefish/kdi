//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/flux/merge_unittest.cc#1 $
//
// Created 2007/09/18
//
// Copyright 2007 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#include "merge.h"
#include "sequence.h"
#include "unittest/main.h"
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
