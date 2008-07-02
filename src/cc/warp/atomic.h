//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/warp/atomic.h#1 $
//
// Created 2006/05/02
//
// Copyright 2006 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
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
