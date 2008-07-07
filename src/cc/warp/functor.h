//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2006 Josh Taylor (Kosmix Corporation)
// Created 2006-06-16
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

#ifndef WARP_FUNCTOR_H
#define WARP_FUNCTOR_H

#include <boost/shared_ptr.hpp>

namespace warp
{
    template <class R>
    class NullaryFunctor;

    template <class R, class A>
    class UnaryFunctor;
}


//----------------------------------------------------------------------------
// NullaryFunctor
//----------------------------------------------------------------------------
template <class R>
class warp::NullaryFunctor
{
public:
    typedef R result_t;
    typedef NullaryFunctor<result_t> my_t;
    typedef boost::shared_ptr<my_t> handle_t;
    typedef handle_t base_handle_t;
    enum { ARITY = 0 };

public:
    virtual ~NullaryFunctor() {}
    virtual result_t call() const = 0;

    inline result_t operator()() const {
        return this->call();
    }
};


//----------------------------------------------------------------------------
// UnaryFunctor
//----------------------------------------------------------------------------
template <class R, class A>
class warp::UnaryFunctor
{
public:
    typedef R result_t;
    typedef A arg_t;
    typedef UnaryFunctor<result_t, arg_t> my_t;
    typedef boost::shared_ptr<my_t> handle_t;
    typedef handle_t base_handle_t;
    enum { ARITY = 1 };

public:
    virtual ~UnaryFunctor() {}
    virtual result_t call(arg_t const & arg) const = 0;

    inline result_t operator()(arg_t const & arg) const {
        return this->call(arg);
    }
};


#endif // WARP_FUNCTOR_H
