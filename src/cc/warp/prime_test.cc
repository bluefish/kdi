//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-02-24
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

#include <warp/prime.h>
#include <warp/options.h>
#include <warp/strutil.h>
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
