//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2006 Josh Taylor (Kosmix Corporation)
// Created 2006-05-12
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
