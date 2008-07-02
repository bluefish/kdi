//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/warp/prime.h#1 $
//
// Created 2007/02/24
//
// Copyright 2007 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef WARP_PRIME_H
#define WARP_PRIME_H

#include <stddef.h>

namespace warp
{
    /// Probabilistically check \c n for primality using the
    /// Miller-Rabin test.  Do up to \c k iterations before declaring
    /// primality.  The probability of a composite number being
    /// classified as prime is at most $4^{-k}$.
    /// @fix This implementation usually thinks values of \c n over
    /// 2^32 are composite due to multiplication overflow problems.
    bool isPrime(size_t n, size_t k=20);

    /// Test each odd number greater than or equal to \c n until a
    /// probable prime is found.  The probability of a composite
    /// number being classified as prime is at most $4^{-k}$.
    /// @fix This implementation usually thinks values of \c n over
    /// 2^32 are composite due to multiplication overflow problems.
    size_t nextPrime(size_t n, size_t k=20);
}

#endif // WARP_PRIME_H
