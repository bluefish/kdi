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
