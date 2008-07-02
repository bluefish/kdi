//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/warp/singleton.h#1 $
//
// Created 2006/02/21
//
// Copyright 2006 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
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
