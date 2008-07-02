//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/warp/util.h#1 $
//
// Created 2005/12/01
//
// Copyright 2005 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef WARP_UTIL_H
#define WARP_UTIL_H

#include <stdint.h>
#include <assert.h>
#include <limits>
#include "algorithm.h"
#include "functional.h"

namespace warp
{
    template <class T>
    inline bool isPowerOf2(T num) {
        return num > 0 && (num & (num - 1)) == 0;
    }

    template <class T>
    inline T alignUp(T pos, uintptr_t align) {
        assert(isPowerOf2(align));
        return T((uintptr_t(pos) + align - 1) & ~(align - 1));
    }

    template <class T>
    inline T alignDown(T pos, uintptr_t align) {
        assert(isPowerOf2(align));
        return T((uintptr_t(pos)) & ~(align - 1));
    }

    template <class T>
    inline T alignUpExp(T pos, int alignExponent) {
        assert(alignExponent >= 0 && alignExponent < sizeof(uintptr_t) * 8);
        uintptr_t alignMask = (uintptr_t(1) << alignExponent) - 1;
        return T((uintptr_t(pos) + alignMask) & ~alignMask);
    }

    template <class T>
    inline T alignDownExp(T pos, int alignExponent) {
        assert(alignExponent >= 0 && alignExponent < sizeof(uintptr_t) * 8);
        uintptr_t alignMask = (uintptr_t(1) << alignExponent) - 1;
        return T(uintptr_t(pos) & ~alignMask);
    }

    template <class T, class V>
    inline bool inNumericRange(V v) {
        return (v >= std::numeric_limits<T>::min() &&
                v <= std::numeric_limits<T>::max());
    }

    template <class T>
    inline T clip(T x, T minVal, T maxVal)
    {
        if(x < minVal)
            return minVal;
        else if(maxVal < x)
            return maxVal;
        else
            return x;
    }

    template <class T>
    inline T deserialize(void const * src)
    {
        T ret;
        std::copy(reinterpret_cast<char const *>(src),
                  reinterpret_cast<char const *>(src) + sizeof(T),
                  reinterpret_cast<char *>(&ret));
        return ret;
    }

    template <class T>
    inline void serialize(void * dst, T const & val)
    {
        std::copy(reinterpret_cast<char const *>(&val),
                  reinterpret_cast<char const *>(&val) + sizeof(T),
                  reinterpret_cast<char *>(dst));
    }

    template <class T>
    T byteswap(T val)
    {
        uint8_t * p = reinterpret_cast<uint8_t *>(&val);
        std::reverse(p, p + sizeof(T));
        return val;
    }


    //------------------------------------------------------------------------
    // order
    //------------------------------------------------------------------------
    template <class A, class B>
    inline bool order(A const & a1, A const & a2,
                      B const & b1, B const & b2)
    {
        if (a1 < a2)
            return true;
        else if(a2 < a1)
            return false;
        else
            return b1 < b2;
    }

    template <class A, class B, class C>
    inline bool order(A const & a1, A const & a2,
                      B const & b1, B const & b2,
                      C const & c1, C const & c2)
    {
        if (a1 < a2)
            return true;
        else if(a2 < a1)
            return false;
        else
            return order(b1, b2, c1, c2);
    }

    template <class A, class B, class C, class D>
    inline bool order(A const & a1, A const & a2,
                      B const & b1, B const & b2,
                      C const & c1, C const & c2,
                      D const & d1, D const & d2)
    {
        if (a1 < a2)
            return true;
        else if(a2 < a1)
            return false;
        else
            return order(b1, b2, c1, c2, d1, d2);
    }


    //------------------------------------------------------------------------
    // lexicographical_compare_3way
    //------------------------------------------------------------------------
    template <class It>
    inline int lexicographical_compare_3way(It a0, It a1, It b0, It b1)
    {
        for(;;)
        {
            // Check for end of ranges
            if(a0 == a1)
            {
                // At end of range A, check range B
                if(b0 == b1)
                    // Both ended at same time
                    return 0;
                else
                    // A ended first
                    return -1;
            }
            else if(b0 == b1)
            {
                // B ended first
                return 1;
            }

            // Check value difference
            if(int diff = int(*a0) - int(*b0))
            {
                // Not the same, return difference
                return diff;
            }

            // Still the same, advance to next item
            ++a0;
            ++b0;
        }
    }
}

#endif // WARP_UTIL_H
