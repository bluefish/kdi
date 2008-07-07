//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2006 Josh Taylor (Kosmix Corporation)
// Created 2006-10-23
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

#ifndef WARP_ROLLINGHASH_H
#define WARP_ROLLINGHASH_H

#include <ex/exception.h>

#include <vector>
#include <algorithm>
#include <stdint.h>

namespace warp
{
    /// Traits for a prime modulus of 2^56 - 5 and a 64-bit integer
    /// type.
    struct PrimeModulusTraits64
    {
        typedef uint64_t integer_t;
        enum { MODULUS = 72057594037927931 };
        static inline integer_t mod(integer_t x) { return x % MODULUS; }
    };

    /// Traits for a prime modulus of 2^24 - 3 and a 32-bit integer
    /// type.
    struct PrimeModulusTraits32
    {
        typedef uint32_t integer_t;
        enum { MODULUS = 16777213 };
        static inline integer_t mod(integer_t x) { return x % MODULUS; }
    };

    /// Traits for a modulus of 2^56 and a 64-bit integer type.
    struct Power2ModulusTraits64
    {
        typedef uint64_t integer_t;
        enum {
            MODULUS = 0x0100000000000000,
            MASK    = MODULUS - 1
        };
        static inline integer_t mod(integer_t x) { return x & MASK; }
    };

    /// Traits for a modulus of 2^24 and a 32-bit integer type.
    struct Power2ModulusTraits32
    {
        typedef uint32_t integer_t;
        enum {
            MODULUS = 0x01000000,
            MASK    = MODULUS - 1
        };
        static inline integer_t mod(integer_t x) { return x & MASK; }
    };

    class FixedWindow;

    template <size_t SZ>
    class StaticWindow;

    template <class MOD, class WINDOW=FixedWindow>
    class RollingHash;
}

//----------------------------------------------------------------------------
// FixedWindow
//----------------------------------------------------------------------------
/// A fixed-size window implemented as a circular buffer.
class warp::FixedWindow
{
public:
    typedef uint8_t byte_t;

private:
    std::vector<byte_t> window;
    size_t sz;
    size_t idx;

public:
    FixedWindow(size_t sz) :
        window(sz,0), sz(sz), idx(0)
    {
        using namespace ex;
        if(sz < 1)
            raise<ValueError>("need positive window size");
    }

    /// Get contents of current window.
    void getWindow(void * dst) const
    {
        memcpy(dst, &window[idx], sz - idx);
        memcpy((char *)dst + sz - idx, &window[0], idx);
    }

    /// Zero contents of window
    void clear()
    {
        std::fill(window.begin(), window.end(), 0);
    }

    /// Get element at back of window.  This is the least recently
    /// pushed element.
    byte_t getBack() const
    {
        return window[idx];
    }

    /// Push a new element at the front of the window, removing
    /// the element at the back.
    void pushFront(byte_t x)
    {
        window[idx] = x;
        if(++idx == sz)
            idx -= sz;
    }

    /// Get size of the window.
    size_t getWindowSize() const
    {
        return sz;
    }
};


//----------------------------------------------------------------------------
// StaticWindow
//----------------------------------------------------------------------------
/// A fixed-size window implemented as a circular buffer.  The
/// size of the window must be fixed at compile time.  This
/// implementation is slightly faster than FixedWindow, but
/// obviously less general.
template <size_t SZ>
class warp::StaticWindow
{
public:
    typedef uint8_t byte_t;
    enum { WINDOW_SIZE = SZ };

private:
    BOOST_STATIC_ASSERT(SZ > 0);
    byte_t window[SZ];
    size_t idx;

public:
    StaticWindow() :
        idx(0)
    {
        clear();
    }

    /// Get contents of current window
    void getWindow(void * dst) const
    {
        memcpy(dst, &window[idx], SZ - idx);
        memcpy((char *)dst + SZ - idx, window, idx);
    }

    /// Zero contents of window
    void clear()
    {
        std::fill(window, window + SZ, 0);
    }

    /// Get element at back of window.  This is the least recently
    /// pushed element.
    byte_t getBack() const
    {
        return window[idx];
    }

    /// Push a new element at the front of the window, removing
    /// the element at the back.
    void pushFront(byte_t x)
    {
        window[idx] = x;
        if(++idx == SZ)
            idx -= SZ;
    }

    /// Get size of the window
    size_t getWindowSize() const
    {
        return SZ;
    }
};


//----------------------------------------------------------------------------
// RollingHash
//----------------------------------------------------------------------------
/// Compute a hash over a fixed window of bytes.  The window can be
/// advanced by one byte in constant time.  The rolling hash will be
/// computed using modular arithmetic where the modulus is given by
/// the \c MOD template parameter.  The storage used to maintain the
/// window is defined by the \c WINDOW template parameter.
template <class MOD, class WINDOW> 
class warp::RollingHash : public WINDOW
{
public:
    typedef MOD modulus_t;
    typedef WINDOW window_t;
    typedef window_t super;
    enum { MODULUS = modulus_t::MODULUS };
    typedef typename modulus_t::integer_t integer_t;

private:
    // This code requires 8 bits for carry information before reducing
    // by the modulus.  Therefore, the maximum modulus size is the
    // 2^(B-8), where B is the number of bits in the hash type.
    BOOST_STATIC_ASSERT(MODULUS <= (integer_t(1) << (sizeof(integer_t)*8-8)));

    integer_t sum;
    integer_t endFactor;

    inline integer_t mod(integer_t x)
    {
        return modulus_t::mod(x);
    }

    inline void computeEndFactor()
    {
        endFactor = 1;
        for(size_t i = 0; i < getWindowSize(); ++i)
            endFactor = mod(endFactor << 8);
    }

public:
    RollingHash()
    {
        computeEndFactor();
        clear();
    }

    template <class WP>
    explicit RollingHash(WP windowParam) :
        super(windowParam)
    {
        computeEndFactor();
        clear();
    }

    /// Get the hash of the current window.
    integer_t getHash() const { return sum; }

    /// Update the window by adding a byte.  The last byte on the
    /// other side of the window will be dropped.
    void update(uint8_t x)
    {
        // Update sum: sum = 256*sum + x - 256^w * window[-1]  (mod q)
        sum = (sum << 8) + x;
        integer_t neg = integer_t(getBack()) * endFactor;
        if(sum >= neg)
            sum = mod(sum - neg);
        else
            sum = MODULUS - mod(neg - sum);

        // Update window
        pushFront(x);
    }

    /// Update many bytes at a time.
    void update(void const * src, size_t len)
    {
        uint8_t const * p = reinterpret_cast<uint8_t const *>(src);

        // Only consider the final window.  Only the last W bytes can
        // constribute to the hash.  There's no need to worry about
        // clearing out the old window and sum -- if we do W updates,
        // it will push out all old information.
        size_t sz = getWindowSize();
        if(len > sz)
        {
            p += len - sz;
            len = sz;
        }

        // Update window
        for(size_t i = 0; i < len; ++i)
            update(*p++);
    }

    /// Reset hash and zero contents of window
    void clear()
    {
        super::clear();
        sum = 0;
    }

    using super::getBack;
    using super::pushFront;
    using super::getWindowSize;
};

#endif // WARP_ROLLINGHASH_H
