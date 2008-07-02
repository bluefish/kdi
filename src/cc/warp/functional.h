//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/warp/functional.h#1 $
//
// Created 2007/07/21
//
// Copyright 2007 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef WARP_FUNCTIONAL_H
#define WARP_FUNCTIONAL_H

#include <functional>

namespace warp
{
    //------------------------------------------------------------------------
    // delete_ptr
    //------------------------------------------------------------------------
    /// Unary functor that deletes its argument with \c delete.
    struct delete_ptr
    {
        template <class T>
        void operator()(T * t) const { delete t; }
    };

    //------------------------------------------------------------------------
    // delete_arr
    //------------------------------------------------------------------------
    /// Unary functor that deletes its argument with \c delete[].
    struct delete_arr
    {
        template <class T>
        void operator()(T * t) const { delete[] t; }
    };

    //------------------------------------------------------------------------
    // size_of
    //------------------------------------------------------------------------
    /// Unary functor that returns the \c sizeof its argument.
    struct size_of
    {
        template <class T>
        size_t operator()(T const & x) const { return sizeof(T); }
    };

    //------------------------------------------------------------------------
    // less
    //------------------------------------------------------------------------
    /// Binary functor that returns true iff its first argument is
    /// less than its second argument, as defined by the less than
    /// operator.
    struct less
    {
        template <class A, class B>
        bool operator()(A const & a, B const & b) const
        {
            return a < b;
        }
    };

    //------------------------------------------------------------------------
    // equal_to
    //------------------------------------------------------------------------
    /// Binary functor that returns true iff its first argument is
    /// equal to its second argument, as defined by the equals
    /// operator.
    struct equal_to
    {
        template <class A, class B>
        bool operator()(A const & a, B const & b) const
        {
            return a == b;
        }
    };

    //------------------------------------------------------------------------
    // reverse_order
    //------------------------------------------------------------------------
    /// A reverse ordering built on a standard less-than comparitor.
    /// The order is inverted by swapping the arguments to the
    /// original comparitor.  This is particularly useful for making
    /// the STL heap algorithms implement a min heap (the default is a
    /// max heap).
    template <class T, class Lt=std::less<T> >
    struct reverse_order
    {
        Lt lt;
        explicit reverse_order(Lt const & lt = Lt()) : lt(lt) {}
        bool operator()(T const & a, T const & b) const { return lt(b,a); }
    };
    
    //------------------------------------------------------------------------
    // begin_pos
    //------------------------------------------------------------------------
    /// Functor to return the begin position in a STL-like container.
    template <class Seq>
    struct begin_pos
    {
        typename Seq::iterator operator()(Seq & s) const
        {
            return s.begin();
        }
        typename Seq::const_iterator operator()(Seq const & s) const
        {
            return s.begin();
        }
    };

    //------------------------------------------------------------------------
    // end_pos
    //------------------------------------------------------------------------
    /// Functor to return the end position in a STL-like container.
    template <class Seq>
    struct end_pos
    {
        typename Seq::iterator operator()(Seq & s) const
        {
            return s.end();
        }
        typename Seq::const_iterator operator()(Seq const & s) const
        {
            return s.end();
        }
    };

    //------------------------------------------------------------------------
    // back_pos
    //------------------------------------------------------------------------
    /// Functor to return the back position (end - 1) in a STL-like container.
    template <class Seq>
    struct back_pos
    {
        typename Seq::iterator operator()(Seq & s) const
        {
            typename Seq::iterator i(s.end());            
            return --i;
        }
        typename Seq::const_iterator operator()(Seq const & s) const
        {
            typename Seq::const_iterator i(s.end());
            return --i;
        }
    };
}

#endif // WARP_FUNCTIONAL_H
