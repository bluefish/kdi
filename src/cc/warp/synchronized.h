//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
// Created 2008-04-09
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

#ifndef WARP_SYNCHRONIZED_H
#define WARP_SYNCHRONIZED_H

#include <boost/noncopyable.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/preprocessor/repetition.hpp>

#ifndef WARP_SYNCHRONIZED_MAX_PARAMS
#  define WARP_SYNCHRONIZED_MAX_PARAMS 5
#endif

namespace warp {

    /// A synchronized wrapper around an object.  The object will be
    /// hidden and can only be accessed in an exclusive fashion
    /// through a LockedPtr, below.
    template <class T>
    class Synchronized;

    /// A exclusive handle to an object wrapped by Synchronized.  Only
    /// one LockedPtr can exist at a time per Synchronized object.
    /// The constructor of a second LockedPtr will block until the
    /// first is destroyed.  LockedPtrs are typically used in local
    /// scope and discarded as soon as possible to allow other threads
    /// to access the Synchronized object.
    template <class T>
    class LockedPtr;

} // namespace warp


//----------------------------------------------------------------------------
// Synchronized
//----------------------------------------------------------------------------
template <class T>
class warp::Synchronized
    : private boost::noncopyable
{
    T object;
    mutable boost::mutex mutex;
    
    template <class U>
    friend class LockedPtr;

public:
    // Default constructor
    Synchronized() : object(), mutex() {}

    // Constructor for 1 to WARP_SYNCHRONIZED_MAX_PARAMS arguments
    #undef SynchronizedConstructor
    #define SynchronizedConstructor(z,n,unused)         \
                                                        \
    template <BOOST_PP_ENUM_PARAMS(n, class A)>         \
    explicit Synchronized(                              \
        BOOST_PP_ENUM_BINARY_PARAMS(n, A, const & a)    \
        ) :                                             \
        object(BOOST_PP_ENUM_PARAMS(n, a)),             \
        mutex()                                         \
    {                                                   \
    }

    BOOST_PP_REPEAT_FROM_TO(1, BOOST_PP_ADD(WARP_SYNCHRONIZED_MAX_PARAMS, 1),
                            SynchronizedConstructor, ~)
    #undef SynchronizedConstructor
};


//----------------------------------------------------------------------------
// LockedPtr
//----------------------------------------------------------------------------
template <class T>
class warp::LockedPtr
    : private boost::noncopyable
{
    boost::mutex::scoped_lock lock;
    T & ref;

public:
    /// Acquire an exclusive handle to the given synchronized object.
    /// This constructor will block if another LockedPtr for the same
    /// object already exists.
    template <class U>
    explicit LockedPtr(Synchronized<U> & synchronizedObject) :
        lock(synchronizedObject.mutex),
        ref(synchronizedObject.object) {}

    /// Acquire an exclusive handle to the given synchronized object.
    /// This constructor will block if another LockedPtr for the same
    /// object already exists.
    template <class U>
    explicit LockedPtr(Synchronized<U> const & synchronizedObject) :
        lock(synchronizedObject.mutex),
        ref(synchronizedObject.object) {}

    T & operator*() const { return ref; }
    T * operator->() const { return &ref; }
};


#endif // WARP_SYNCHRONIZED_H
