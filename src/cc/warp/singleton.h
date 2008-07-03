//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2006 Josh Taylor (Kosmix Corporation)
// Created 2006-02-21
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

#ifndef WARP_SINGLETON_H
#define WARP_SINGLETON_H

namespace warp
{
    template <class DerivedType>
    class Singleton;
}

//----------------------------------------------------------------------------
// Singleton
//----------------------------------------------------------------------------
template <class DerivedType>
class warp::Singleton
{
protected:
    typedef Singleton<DerivedType> friend_t;
    Singleton() {}
private:
    Singleton(Singleton const & o);
    Singleton const & operator=(Singleton const & o);
public:
    static DerivedType & get()
    {
        static DerivedType theOne;
        return theOne;
    }
};


#endif // WARP_SINGLETON_H
