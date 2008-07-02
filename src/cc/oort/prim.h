//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/oort/prim.h#1 $
//
// Created 2006/01/11
//
// Copyright 2006 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef OORT_PRIM_H
#define OORT_PRIM_H

#include "warp/util.h"
#include <algorithm>

namespace oort
{
    //------------------------------------------------------------------------
    // Swapped
    //------------------------------------------------------------------------
    template <class T>
    class Swapped
    {
    public:
        typedef T value_type;
        
    private:
        T data;

    public:
        Swapped() {}
        Swapped(T orig) : data(warp::byteswap(orig)) {}
        Swapped(Swapped<T> const & o) : data(o.data) {}
        Swapped const & operator=(T orig)
        {
            data = warp::byteswap(orig);
            return *this;
        }
        Swapped const & operator=(Swapped<T> const & o)
        {
            data = o.data;
            return *this;
        }
        T get() const { return warp::byteswap(data); }
        operator T() const { return get(); }
    };


    //------------------------------------------------------------------------
    // Packed
    //------------------------------------------------------------------------
    template <class T>
    class Packed
    {
    public:
        typedef T value_type;
        
    private:
        char data[sizeof(T)];

    public:
        Packed() {}
        Packed(T orig) { warp::serialize(data, orig); }
        Packed(Packed<T> const & o) {
            std::copy(o.data, o.data+sizeof(T), data);
        }
        Packed const & operator=(T orig)
        {
            warp::serialize(data, orig);
            return *this;
        }
        Packed const & operator=(Packed<T> const & o)
        {
            std::copy(o.data, o.data+sizeof(T), data);
            return *this;
        }
        T get() const { return warp::deserialize<T>(data); }
        operator T() const { return get(); }
    };


    //------------------------------------------------------------------------
    // PackedSwapped
    //------------------------------------------------------------------------
    template <class T>
    class PackedSwapped : public Packed< Swapped<T> >
    {
    public:
        typedef T value_type;
        typedef Packed< Swapped<T> > super;

    public:
        PackedSwapped() {}
        PackedSwapped(T orig) : super(Swapped<T>(orig)) {}
        PackedSwapped const & operator=(T orig) {
            *this = Swapped<T>(orig);
            return *this;
        }
        T get() const { return super::get().get(); }
        operator T() const { return get(); }
    };
}

#endif // OORT_PRIM_H
