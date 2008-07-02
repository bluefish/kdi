//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/warp/hsieh_hash.h#1 $
//
// Created 2006/08/04
//
// Copyright 2006 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef WARP_HSIEH_HASH_H
#define WARP_HSIEH_HASH_H

#include <stdint.h>
#include <stddef.h>

namespace warp
{
    uint32_t hsieh_hash(void const * data, size_t len, uint32_t hash);

    inline uint32_t hsieh_hash(void const * data, size_t len)
    {
        return hsieh_hash(data, len, uint32_t(len));
    }

    inline uint32_t hsieh_hash(char const * begin, char const * end)
    {
        return hsieh_hash(begin, end - begin);
    }
}

#endif // WARP_HSIEH_HASH_H
