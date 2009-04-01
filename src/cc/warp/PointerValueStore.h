//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
// Created 2009-03-31
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

#ifndef WARP_POINTERVALUESTORE_H
#define WARP_POINTERVALUESTORE_H

#include <warp/objectpool.h>

namespace warp {

    template <class T, bool embedded=(sizeof(T)<=sizeof(void*))>
    class PointerValueStore;

    template <class T>
    class PointerValueStore<T, true>
    {
    public:
        template <class P>
        T & ref(P * & p)
        {
            return reinterpret_cast<T &>(p);
        }

        template <class P>
        T cref(P const * p)
        {
            return reinterpret_cast<T>(p);
        }

        template <class P>
        void write(T const & v, P * & p)
        {
            ref(p) = v;
        }
    };

    template <class T>
    class PointerValueStore<T, false>
    {
        ObjectPool<T> valuePool;

    public:
        T & ref(void * p)
        {
            return *reinterpret_cast<T *>(p);
        }

        T const & cref(void const * p)
        {
            return *reinterpret_cast<T const *>(p);
        }

        void write(T const & v, void * & p)
        {
            if(p)
                ref(p) = v;
            else
                p = valuePool.construct(v);
        }
    };

} // namespace warp

#endif // WARP_POINTERVALUESTORE_H
