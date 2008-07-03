//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
// Created 2008-02-15
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

#ifndef WARP_BIT_VECTOR_H
#define WARP_BIT_VECTOR_H

#include <boost/scoped_array.hpp>
#include <stdint.h>
#include <stddef.h>

namespace warp {

    class BitVector;

} // namespace warp

//----------------------------------------------------------------------------
// BitVector
//----------------------------------------------------------------------------
class warp::BitVector
{
    boost::scoped_array<size_t> buffer;
    size_t nBits;

    /// Return the number of bytes required to hold the given number
    /// of bits.
    static size_t bitsToBytes(size_t nBits)
    {
        return (nBits + 7ul) >> 3;
    }

    /// Get the byte containing the given vector bit index.
    uint8_t & byte(size_t i)
    {
        return reinterpret_cast<uint8_t *>(buffer.get())[i >> 3];
    }

    /// Get the byte containing the given vector bit index.
    uint8_t byte(size_t i) const
    {
        return reinterpret_cast<uint8_t const *>(buffer.get())[i >> 3];
    }

    /// Get a byte where the \c i-th bit (mod 8) is set and all the
    /// others are clear.
    static uint8_t bit(size_t i)
    {
        return 1u << (i & 7ul);
    }

public:
    /// Create a bit vector with the given number of bits.  All bits
    /// are cleared.
    explicit BitVector(size_t nBits);

    /// Copy constructor
    BitVector(BitVector const & o);

    /// Assignment
    BitVector & operator=(BitVector const & o);

    /// Get the number of bits in the vector.
    size_t size() const { return nBits; }

    /// Set the \c i-th bit in the vector.
    void set(size_t i) { byte(i) |= bit(i);}

    /// Clear the \c i-th bit in the vector.
    void clear(size_t i) { byte(i) &= ~bit(i); }

    /// Flip/toggle the \c i-th bit in the vector.
    void flip(size_t i) { byte(i) ^= bit(i); }

    /// Test to see if the \c i-th bit in the vector is set.
    bool test(size_t i) const { return byte(i) & bit(i); }

    /// Clear all bits in the vector.
    void clear();

    /// Count the number of set bits in the vector.
    size_t population() const;

    /// Get the raw bytes from the bit vector, rounded up to the
    /// nearest byte.  The i-th bit is located in the (i%8)-th
    /// position of the (i/8)-th byte.  Bit values after size() bits
    /// are undefined.
    uint8_t const * raw() const
    {
        return reinterpret_cast<uint8_t const *>(buffer.get());
    }

    /// Get the number of bytes used for the raw representation.
    size_t rawSize() const { return bitsToBytes(nBits); }

    /// Load a bit vector from a raw buffer.  The buffer should
    /// contain bits such that the i-th bit is located in the (i%8)-th
    /// position of the (i/8)-th byte.
    void load(size_t nBits, void const * raw);

    /// Bitwise-OR assignment.  Both vectors must have the same size.
    BitVector & operator|=(BitVector const & o);

    /// Bitwise-AND assignment.  Both vectors must have the same size.
    BitVector & operator&=(BitVector const & o);

    /// Bitwise-XOR assignment.  Both vectors must have the same size.
    BitVector & operator^=(BitVector const & o);

    /// Bitwise-OR copy.  Both vectors must have the same size.
    BitVector operator|(BitVector const & o) const
    {
        return BitVector(*this) |= o;
    }

    /// Bitwise-AND copy.  Both vectors must have the same size.
    BitVector operator&(BitVector const & o) const
    {
        return BitVector(*this) &= o;
    }

    /// Bitwise-XOR copy.  Both vectors must have the same size.
    BitVector operator^(BitVector const & o) const
    {
        return BitVector(*this) ^= o;
    }
};


#endif // WARP_BIT_VECTOR_H
