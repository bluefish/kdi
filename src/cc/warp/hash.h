//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/warp/hash.h#1 $
//
// Created 2006/05/12
//
// Copyright 2006 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef WARP_HASH_H
#define WARP_HASH_H

#include <stdint.h>
#include <stddef.h>

namespace warp
{
    //------------------------------------------------------------------------
    // hash
    //------------------------------------------------------------------------
    template <class T> struct hash {};

    template <> struct hash<int8_t>
    {
        typedef int8_t param_t;
        typedef size_t result_t;
        result_t operator()(param_t p) const { return result_t(p); }
    };

    template <> struct hash<uint8_t>
    {
        typedef uint8_t param_t;
        typedef size_t result_t;
        result_t operator()(param_t p) const { return result_t(p); }
    };

    template <> struct hash<int16_t>
    {
        typedef int16_t param_t;
        typedef size_t result_t;
        result_t operator()(param_t p) const { return result_t(p); }
    };

    template <> struct hash<uint16_t>
    {
        typedef uint16_t param_t;
        typedef size_t result_t;
        result_t operator()(param_t p) const { return result_t(p); }
    };

    template <> struct hash<int32_t>
    {
        typedef int32_t param_t;
        typedef size_t result_t;
        result_t operator()(param_t p) const { return result_t(p); }
    };

    template <> struct hash<uint32_t>
    {
        typedef uint32_t param_t;
        typedef size_t result_t;
        result_t operator()(param_t p) const { return result_t(p); }
    };

    template <> struct hash<int64_t>
    {
        typedef int64_t param_t;
        typedef size_t result_t;
        result_t operator()(param_t p) const { return result_t(p); }
    };

    template <> struct hash<uint64_t>
    {
        typedef uint64_t param_t;
        typedef size_t result_t;
        result_t operator()(param_t p) const { return result_t(p); }
    };

    template <class T> struct hash<T *>
    {
        typedef T * param_t;
        typedef size_t result_t;
        result_t operator()(param_t p) const { return result_t(p); }
    };

    //------------------------------------------------------------------------
    // result_of_hash
    //------------------------------------------------------------------------
    template <class H> struct result_of_hash {};
    
    template <class T> struct result_of_hash< hash<T> >
    {
        typedef typename hash<T>::result_t type;
    };
}

#endif // WARP_HASH_H
