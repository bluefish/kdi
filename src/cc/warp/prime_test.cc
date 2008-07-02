//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/warp/prime_test.cc#1 $
//
// Created 2007/02/24
//
// Copyright 2007 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#include "prime.h"
#include "options.h"
#include "strutil.h"
#include <iostream>

using namespace warp;
using namespace std;

int main(int ac, char ** av)
{
    using boost::program_options::value;
    OptionParser op;
    op.addOption("iterations,k", value<size_t>()->default_value(20),
                 "Iterations for primality testing");
    op.addOption("findnext,n", "Find next prime above composite numbers");
    
    OptionMap opt;
    ArgumentList args;
    op.parseOrBail(ac, av, opt, args);

    size_t k = 20;
    opt.get("iterations", k);

    bool next = hasopt(opt, "findnext");

    for(ArgumentList::const_iterator ai = args.begin();
        ai != args.end(); ++ai)
    {
        size_t n = parseInt<size_t>(wrap(*ai));

        cout << "n=" << n << ':';
        cout.flush();
        
        if(isPrime(n, k))
            cout << " prime" << endl;
        else if(next)
        {
            cout << " composite";
            cout.flush();
            cout << ", next prime=" << nextPrime(n,k) << endl;
        }
        else
            cout << " composite" << endl;
    }

    return 0;
}
