//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/warp/shared.h#1 $
//
// Created 2007/04/16
//
// Copyright 2007 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef WARP_SHARED_H
#define WARP_SHARED_H

#include <boost/shared_ptr.hpp>

namespace warp
{
    /// Construct a new object in a shared_ptr.
    template <class T>
    boost::shared_ptr<T>
    newShared()
    {
        boost::shared_ptr<T> p(new T);
        return p;
    }

    /// Construct a new object in a shared_ptr.
    template <class T, class A1>
    boost::shared_ptr<T>
    newShared(A1 const & a1)
    {
        boost::shared_ptr<T> p(new T(a1));
        return p;
    }

    /// Construct a new object in a shared_ptr.
    template <class T, class A1, class A2>
    boost::shared_ptr<T>
    newShared(A1 const & a1, A2 const & a2)
    {
        boost::shared_ptr<T> p(new T(a1, a2));
        return p;
    }

    /// Construct a new object in a shared_ptr.
    template <class T, class A1, class A2, class A3>
    boost::shared_ptr<T>
    newShared(A1 const & a1, A2 const & a2, A3 const & a3)
    {
        boost::shared_ptr<T> p(new T(a1, a2, a3));
        return p;
    }

    /// Construct a new object in a shared_ptr.
    template <class T, class A1, class A2, class A3, class A4>
    boost::shared_ptr<T>
    newShared(A1 const & a1, A2 const & a2, A3 const & a3, A4 const & a4)
    {
        boost::shared_ptr<T> p(new T(a1, a2, a3, a4));
        return p;
    }

    /// Construct a new object in a shared_ptr.
    template <class T, class A1, class A2, class A3, class A4, class A5>
    boost::shared_ptr<T>
    newShared(A1 const & a1, A2 const & a2, A3 const & a3, A4 const & a4,
              A5 const & a5)
    {
        boost::shared_ptr<T> p(new T(a1, a2, a3, a4, a5));
        return p;
    }

    /// Construct a new object in a shared_ptr.
    template <class T, class A1, class A2, class A3, class A4, class A5,
              class A6>
    boost::shared_ptr<T>
    newShared(A1 const & a1, A2 const & a2, A3 const & a3, A4 const & a4,
              A5 const & a5, A6 const & a6)
    {
        boost::shared_ptr<T> p(new T(a1, a2, a3, a4, a5, a6));
        return p;
    }

    /// Construct a new object in a shared_ptr.
    template <class T, class A1, class A2, class A3, class A4, class A5,
              class A6, class A7>
    boost::shared_ptr<T>
    newShared(A1 const & a1, A2 const & a2, A3 const & a3, A4 const & a4,
              A5 const & a5, A6 const & a6, A7 const & a7)
    {
        boost::shared_ptr<T> p(new T(a1, a2, a3, a4, a5, a6, a7));
        return p;
    }

    /// Construct a new object in a shared_ptr.
    template <class T, class A1, class A2, class A3, class A4, class A5,
              class A6, class A7, class A8>
    boost::shared_ptr<T>
    newShared(A1 const & a1, A2 const & a2, A3 const & a3, A4 const & a4,
              A5 const & a5, A6 const & a6, A7 const & a7, A8 const & a8)
    {
        boost::shared_ptr<T> p(new T(a1, a2, a3, a4, a5, a6, a7, a8));
        return p;
    }

    /// Construct a new object in a shared_ptr.
    template <class T, class A1, class A2, class A3, class A4, class A5,
              class A6, class A7, class A8, class A9>
    boost::shared_ptr<T>
    newShared(A1 const & a1, A2 const & a2, A3 const & a3, A4 const & a4,
              A5 const & a5, A6 const & a6, A7 const & a7, A8 const & a8,
              A9 const & a9)
    {
        boost::shared_ptr<T> p(new T(a1, a2, a3, a4, a5, a6, a7, a8, a9));
        return p;
    }
}

#endif // WARP_SHARED_H
