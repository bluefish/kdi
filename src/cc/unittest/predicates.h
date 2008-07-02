//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/unittest/predicates.h#2 $
//
// Created 2007/07/30
//
// Copyright 2007 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef UNITTEST_PREDICATES_H
#define UNITTEST_PREDICATES_H

#include <boost/test/test_tools.hpp>
#include <vector>

namespace unittest
{
    using boost::test_tools::predicate_result;

    /// Check that two unordered collections contain the same values.
    template <class It1, class It2>
    predicate_result
    equal_unordered_collections(It1 a0, It1 a1, It2 b0, It2 b1)
    {
        predicate_result r(true);

        std::vector<It1> a;
        for(It1 i = a0; i != a1; ++i)
            a.push_back(i);
        std::vector<It2> b;
        for(It2 i = b0; i != b1; ++i)
            b.push_back(i);

        if(a.size() != b.size())
        {
            r = false;
            r.message() << "\n  Collections have different sizes: " << a.size()
                        << " != " << b.size();
        }

        typename std::vector<It1>::iterator ai;
        typename std::vector<It2>::iterator bi;

        for(ai = a.begin(); ai != a.end(); ++ai)
        {
            for(bi = b.begin(); bi != b.end(); ++bi)
            {
                if(**ai == **bi)
                    break;
            }
            if(bi != b.end())
                b.erase(bi);
            else
            {
                r = false;
                r.message() << "\n  First collection has extra value: " << **ai;
            }
        }
        for(bi = b.begin(); bi != b.end(); ++bi)
        {
            r = false;
            r.message() << "\n  Second collection has extra value: " << **bi;
        }

        return r;
    }
}

#endif // UNITTEST_PREDICATES_H
