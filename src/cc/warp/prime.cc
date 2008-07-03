//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-02-24
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

#include "prime.h"
#include "ex/exception.h"
#include <boost/random.hpp>

using namespace warp;
using namespace ex;

namespace
{
    typedef boost::mt19937 rng_t;
    typedef boost::uniform_int<size_t> dbn_t;
    typedef boost::variate_generator<rng_t &, dbn_t> randvar_t;

    inline randvar_t uniformRandomVariable(size_t min, size_t max)
    {
        static rng_t rng;
        return randvar_t(rng, dbn_t(min, max));
    }

    inline size_t modpow(size_t base, size_t power, size_t mod)
    {
        size_t result = 1;
        while(power > 0)
        {
            if(power & 1)
            {
                result *= base;
                result %= mod;
            }
            base *= base;
            base %= mod;
            power >>= 1;
        }
        return result;
    }
}

bool warp::isPrime(size_t n, size_t k)
{
    // Handle easy cases
    if(n < 3)
        return n == 2;

    // Reject even numbers
    if((n & 1) == 0)
        return false;

    size_t d = n - 1;
    do {
        d >>= 1;
    } while((d & 1) == 0);

    randvar_t rv = uniformRandomVariable(1, n-1);

    for(; k; --k)
    {
        size_t a = rv();
        size_t t = d;
        size_t y = modpow(a, t, n);

        while(t != n-1 && y != 1 && y != n-1)
        {
            y *= y;
            y %= n;
            t <<= 1;
        }

        if(y != n-1 && (t & 1) == 0)
            return false;
    }

    return true;
}

size_t warp::nextPrime(size_t n, size_t k)
{
    // Handle easy cases
    if(n < 3)
    {
        if(n == 2)
            return 3;
        else
            return 2;
    }

    // isPrime tends to return composite for values over MAX_N
    size_t const MAX_N((size_t(1) << (sizeof(size_t) * 4)) - 1);

    // Test odd numbers until we find a prime or get to the broken region
    for(n = (n & ~size_t(1)) | 1; n <= MAX_N; n += 2)
    {
        if(isPrime(n, k))
            return n;
    }

    // Don't get stuck in a (large) finite loop
    raise<NotImplementedError>("nextPrime() is broken for values of "
                               "N > %d: bailing on %d", MAX_N, n);
}
