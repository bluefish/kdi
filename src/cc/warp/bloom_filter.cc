//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: warp/bloom_filter.cc $
//
// Created 2008/02/15
//
// Copyright 2008 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#include <warp/bloom_filter.h>
#include <warp/util.h>
#include <ex/exception.h>

using namespace warp;
using namespace ex;

BloomFilter::BloomFilter(size_t nBits, size_t nHashes) :
    bits(nBits), seeds(nHashes)
{
    std::generate(seeds.begin(), seeds.end(), rand);
    for(;;)
    {
        // Sort seeds so we can tell if they're equal to another
        // filter's seeds.  It also helps in ensuring the seeds are
        // unique.
        std::sort(seeds.begin(), seeds.end());

        // Make sure seeds are unique
        bool unique = true;
        for(size_t i = 1; i < seeds.size(); ++i)
        {
            if(seeds[i] == seeds[i-1])
            {
                // Oops!  Got two in a row.  Regenerate one of the
                // seeds and do another sort and verify pass.
                unique = false;
                seeds[i-1] = rand();
            }
        }
        if(unique)
            break;
    }
    
}

BloomFilter::BloomFilter(size_t nBits, std::vector<uint32_t> const & seeds_) :
    bits(nBits), seeds(seeds_)
{
    // Sort seeds so we can tell if they're equal to another filter's
    // seeds.
    std::sort(seeds.begin(), seeds.end());
}

BloomFilter::BloomFilter(strref_t buf) :
    bits(0)
{
    load(buf);
}

void BloomFilter::insert(BloomFilter const & o)
{
    if(!hasEqualParameters(o))
        raise<ValueError>("filters have incompatible parameters");
    bits |= o.bits;
}

void BloomFilter::serialize(std::vector<char> & buf) const
{
    // Format: nBits/4 nSeeds/4 (seed/4)*nSeeds bits/((nBits+7)/8)
    size_t sz = 4 + 4 + (4 * seeds.size()) + bits.rawSize();
    buf.resize(sz);

    char * p = &buf[0];
    warp::serialize<uint32_t>(p, bits.size());
    p += 4;

    warp::serialize<uint32_t>(p, seeds.size());
    p += 4;

    for(std::vector<uint32_t>::const_iterator i = seeds.begin();
        i != seeds.end(); ++i)
    {
        warp::serialize(p, *i);
        p += 4;
    }
    
    memcpy(p, bits.raw(), bits.rawSize());
}

void BloomFilter::load(strref_t buf)
{
    if(buf.size() < 8)
        raise<ValueError>("Bloom filter data is too small");

    char const * p = buf.begin();
    size_t nBits = deserialize<uint32_t>(p);
    p += 4;

    size_t nSeeds = deserialize<uint32_t>(p);
    p += 4;

    if(buf.size() != (4 + 4 + (4 * nSeeds) + ((nBits + 7) / 8)))
        raise<ValueError>("Bloom filter data is wrong size");

    seeds.clear();
    seeds.reserve(nSeeds);
    for(size_t i = 0; i < nSeeds; ++i)
    {
        seeds.push_back(deserialize<uint32_t>(p));
        p += 4;
    }

    bits.load(nBits, p);
}
