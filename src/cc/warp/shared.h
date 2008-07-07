//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-04-16
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
