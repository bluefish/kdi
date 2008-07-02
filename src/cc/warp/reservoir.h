// Created 2007/05/07
//
// Copyright 2007 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// @author John Adams
//----------------------------------------------------------------------------

#ifndef WARP_RESERVOIR_H
#define WARP_RESERVOIR_H

#include <vector>
#include <cstdlib>
#include <time.h>

namespace warp {
    template <typename T>
    class ReservoirSampler;
}

//---------------------------------------------------------------------------- 
// ReservoirSampler
//---------------------------------------------------------------------------- 

template <typename T>
class warp::ReservoirSampler
{

public:
    ReservoirSampler(const uint32_t sampleSize, unsigned int seed = time(NULL)) : sampleSize(sampleSize), numSeen(0) {
      srand(seed);
    }

    void add(T const &item) { 
        ++numSeen;
        if (sample.size() < sampleSize) {
            sample.push_back(item);
        }
        else if ((rand() / (RAND_MAX + 1.0)) < ((double) sampleSize / numSeen)) {
            typedef typename std::vector<T>::size_type Offset;
            Offset offset = static_cast<Offset>(
                sampleSize * (rand() / (RAND_MAX + 1.0)));
            sample[offset] = item;
        }
    }

    const std::vector<T> getSample() const {
        return sample;
    }

    uint32_t getSampleSize() const { return sample.size(); }
    uint32_t getNumProcessed() const { return numSeen; }

private:
    uint32_t sampleSize;
    uint32_t numSeen;
    std::vector<T> sample;

};

#endif
