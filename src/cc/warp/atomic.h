//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2006 Josh Taylor (Kosmix Corporation)
// Created 2006-05-02
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

#ifndef WARP_ATOMIC_H
#define WARP_ATOMIC_H

#if (__GNUC__ >= 4)

#include <stdint.h>

namespace warp
{

    class AtomicCounter
    {
        mutable volatile uint32_t x;

    public:
        explicit AtomicCounter(uint32_t x=0) : x(x) {}

        /// Atomically increment counter.
        void increment()
        {
            __sync_add_and_fetch(&x, 1);
        }

        /// Atomically decrement counter and return true iff counter
        /// equals zero.
        bool decrementAndTest()
        {
            return __sync_sub_and_fetch(&x, 1) == 0;
        }

        /// Atomically read the value of the counter.
        int get() const
        {
            return __sync_or_and_fetch(&x, 0);
        }
    };
}


#else

#define CONFIG_SMP 1
#include <asm/atomic.h>

namespace warp
{

    class AtomicCounter
    {
        atomic_t x;
    
    public:
        explicit AtomicCounter(int i=0)
        {
            atomic_set(&x, i);
        }

        /// Atomically increment counter.
        void increment()
        {
            atomic_inc(&x);
        }

        /// Atomically decrement counter and return true iff counter
        /// equals zero.
        bool decrementAndTest()
        {
            return atomic_dec_and_test(&x) != 0;
        }

        /// Atomically read the value of the counter.
        int get() const
        {
            return atomic_read(&x);
        }
    };
}

#endif

#endif // WARP_ATOMIC_H
