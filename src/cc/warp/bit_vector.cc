//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: warp/bit_vector.cc $
//
// Created 2008/02/15
//
// Copyright 2008 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#include <warp/bit_vector.h>
#include <warp/util.h>

extern "C" {
#include <string.h> // mem* functions
}

using namespace warp;

namespace {

    uint8_t const BYTE_POPULATION[] = {
        0,1,1,2,1,2,2,3,1,2,2,3,2,3,3,4,1,2,2,3,2,3,3,4,2,3,3,4,3,4,4,5,
        1,2,2,3,2,3,3,4,2,3,3,4,3,4,4,5,2,3,3,4,3,4,4,5,3,4,4,5,4,5,5,6,
        1,2,2,3,2,3,3,4,2,3,3,4,3,4,4,5,2,3,3,4,3,4,4,5,3,4,4,5,4,5,5,6,
        2,3,3,4,3,4,4,5,3,4,4,5,4,5,5,6,3,4,4,5,4,5,5,6,4,5,5,6,5,6,6,7,
        1,2,2,3,2,3,3,4,2,3,3,4,3,4,4,5,2,3,3,4,3,4,4,5,3,4,4,5,4,5,5,6,
        2,3,3,4,3,4,4,5,3,4,4,5,4,5,5,6,3,4,4,5,4,5,5,6,4,5,5,6,5,6,6,7,
        2,3,3,4,3,4,4,5,3,4,4,5,4,5,5,6,3,4,4,5,4,5,5,6,4,5,5,6,5,6,6,7,
        3,4,4,5,4,5,5,6,4,5,5,6,5,6,6,7,4,5,5,6,5,6,6,7,5,6,6,7,6,7,7,8
    };

    // Return the number of size_t words required to hold the given
    // number of bits.
    inline size_t bitsToWords(size_t nBits)
    {
        return (nBits + (sizeof(size_t)*8 - 1)) / sizeof(size_t);
    }

    inline size_t * allocBits(size_t nBits)
    {
        if(nBits)
            return new size_t[bitsToWords(nBits)];
        else
            return 0;
    }
}

BitVector::BitVector(size_t nBits) :
    buffer(allocBits(nBits)),
    nBits(nBits)
{
    clear();
}

BitVector::BitVector(BitVector const & o) :
    buffer(new size_t[bitsToWords(o.nBits)]),
    nBits(o.nBits)
{
    memcpy(buffer.get(), o.buffer.get(), bitsToBytes(nBits));
}

BitVector & BitVector::operator=(BitVector const & o)
{
    buffer.reset(new size_t[bitsToWords(o.nBits)]);
    nBits = o.nBits;
    memcpy(buffer.get(), o.buffer.get(), bitsToBytes(nBits));
    return *this;
}

void BitVector::clear()
{
    memset(buffer.get(), 0, bitsToBytes(nBits));
}

size_t BitVector::population() const
{
    size_t n = 0;
    for(uint8_t const *i = raw(), *end = i + bitsToBytes(nBits); i != end; ++i)
        n += BYTE_POPULATION[*i];
    return n;
}

void BitVector::load(size_t nBits, void const * raw)
{
    this->nBits = nBits;
    buffer.reset(allocBits(nBits));
    memcpy(buffer.get(), raw, bitsToBytes(nBits));
    if(nBits & 7ul)
        byte(nBits-1) &= ~(0xffu << (nBits & 7ul));
}

BitVector & BitVector::operator|=(BitVector const & o)
{
    size_t * dst = buffer.get();
    size_t const * src = o.buffer.get();
    size_t n = bitsToWords(nBits);
    while(n--)
        *dst++ |= *src++;
    return *this;
}

BitVector & BitVector::operator&=(BitVector const & o)
{
    size_t * dst = buffer.get();
    size_t const * src = o.buffer.get();
    size_t n = bitsToWords(nBits);
    while(n--)
        *dst++ &= *src++;
    return *this;
}

BitVector & BitVector::operator^=(BitVector const & o)
{
    size_t * dst = buffer.get();
    size_t const * src = o.buffer.get();
    size_t n = bitsToWords(nBits);
    while(n--)
        *dst++ ^= *src++;
    return *this;
}
