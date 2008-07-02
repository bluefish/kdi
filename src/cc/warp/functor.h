//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/warp/functor.h#1 $
//
// Created 2006/06/16
//
// Copyright 2006 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
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
