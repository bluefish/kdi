//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: warp/bloom_filter.h $
//
// Created 2008/02/15
//
// Copyright 2008 Kosmix Corporation.  All rights reserved.
// Kosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef WARP_BLOOM_FILTER_H
#define WARP_BLOOM_FILTER_H

#include <warp/bit_vector.h>
#include <warp/strref.h>
#include <vector>
#include <stddef.h>

namespace warp {

    class BloomFilter;

} // namespace warp

//----------------------------------------------------------------------------
// BloomFilter
//----------------------------------------------------------------------------
class warp::BloomFilter
{
public:
    /// Order BloomFilters by parameters.  Filters are first compared
    /// by the number of bits, and then by the hash seed values.
    /// Filters with equal values under this relationship can be
    /// merged with simple bitwise OR of their bit vectors.
    struct ParameterLt;

private:
    BitVector bits;
    std::vector<uint32_t> seeds;

    /// Compute a simple SHIFT-ADD-XOR hash of some data.  The initial
    /// state is given in the optional argument.  This hash function
    /// is very simple and the state is the hash, so incremental
    /// updates can be performed by using previous hashes as the
    /// initial state.  That is, hash(X+Y) == hash(Y, hash(X)).
    static size_t hash(void const * data, size_t len,
                       size_t init=0x79f862aac9cc5a8e)
    {
        size_t x = init;
        for(char const *p = static_cast<char const *>(data), *end = p + len;
            p != end; ++p)
        {
            x ^= (x << 7) + (x >> 3) + size_t(*p);
        }
        return x;
    }

    /// Get a bit position by hashing the given data with a seed
    /// value.
    size_t getBit(str_data_t const & x, uint32_t seed) const
    {
        // Hash the seed first to twiddle the initial state
        size_t h = hash(&seed, sizeof(seed));

        // Hash the data
        h = hash(x.begin(), x.size(), h);

        // Get a bit position from the hash
        return h % bits.size();
    }

public:
    /// Construct a Bloom filter with the given number of bits and
    /// hashes.  The seeds for the hash functions will be randomly
    /// generated.
    BloomFilter(size_t nBits, size_t nHashes);

    /// Construct a Bloom filter with the given number of bits and
    /// a number of hashes equal to the number of given seeds.
    BloomFilter(size_t nBits, std::vector<uint32_t> const & seeds);

    /// Construct a Bloom filter from previously serialized data.
    explicit BloomFilter(strref_t buf);

    /// Hash some data and insert it into the Bloom filter.
    void insert(strref_t x)
    {
        for(std::vector<uint32_t>::const_iterator i = seeds.begin();
            i != seeds.end(); ++i)
        {
            bits.set(getBit(x, *i));
        }
    }

    /// Insert all the items from the given Bloom filter into this
    /// one.  For this to work, the Bloom filters must have the same
    /// parameters: they must have an equal number of bits and
    /// identical hash seeds.
    /// @see hasEqualParameters()
    void insert(BloomFilter const & o);

    /// Get the number of bits used for this filter.
    size_t getBitCount() const { return bits.size(); }

    /// Get the seeds used for this filter.
    std::vector<uint32_t> getHashSeeds() const { return seeds; }

    /// Return true if the given Bloom filter has parameters
    /// compatible with this one.  That is, they have an equal number
    /// of bits and identical hash seeds.
    bool hasEqualParameters(BloomFilter const & o) const
    {
        return (bits.size() == o.bits.size() &&
                seeds.size() == o.seeds.size() &&
                std::equal(seeds.begin(), seeds.end(), o.seeds.begin()));
    }

    /// Clear the contents of Bloom filter.
    void clear()
    {
        bits.clear();
    }

    /// Check to see if the given data has previously been inserted in
    /// the Bloom filter.  If this function returns false, it is
    /// guaranteed that the item isn't in the filter.  If it returns
    /// true, the item probably is in the filter, though there is a
    /// chance of false positives.  The probability of false positives
    /// depends on the number of bits in the filter, the number of
    /// items inserted into the filter, and the number of hashes used.
    /// For a more complete discussion, see
    /// http://pages.cs.wisc.edu/~cao/papers/summary-cache/node8.html
    bool contains(strref_t x) const
    {
        for(std::vector<uint32_t>::const_iterator i = seeds.begin();
            i != seeds.end(); ++i)
        {
            if(!bits.test(getBit(x, *i)))
                return false;
        }
        return true;
    }

    /// Serialize the state of the Bloom filter into a buffer.
    void serialize(std::vector<char> & buf) const;

    /// Load a previously serialized Bloom filter.
    void load(strref_t buf);
};


//----------------------------------------------------------------------------
// BloomFilter::ParameterLt
//----------------------------------------------------------------------------
struct warp::BloomFilter::ParameterLt
{
    bool operator()(BloomFilter const & a, BloomFilter const & b) const
    {
        if(a.bits.size() != b.bits.size())
            return a.bits.size() < b.bits.size();
        else
            return std::lexicographical_compare(
                a.seeds.begin(), a.seeds.end(),
                b.seeds.begin(), b.seeds.end());
    }
};


#endif // WARP_BLOOM_FILTER_H
