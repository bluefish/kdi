//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-07-21
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

#ifndef WARP_FUNCTIONAL_H
#define WARP_FUNCTIONAL_H

#include <functional>
#include <stddef.h>

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
    // unary_constant
    //------------------------------------------------------------------------
    /// Unary functor that returns a constant.
    template <class R, R K>
    struct unary_constant
    {
        template <class A>
        R operator()(A const &) const { return K; }
    };

    //------------------------------------------------------------------------
    // no_op
    //------------------------------------------------------------------------
    /// Functor that does nothing for up to 2 arguments
    struct no_op
    {
        void operator()() const {}

        template <class A>
        void operator()(A const &) const {}

        template <class A, class B>
        void operator()(A const &, B const &) const {}
    };

    //------------------------------------------------------------------------
    // logical_not
    //------------------------------------------------------------------------
    /// Functor that returns the logical negation of another functor.
    /// This functor simply applies the logical negation operator
    /// (operator!) to the result of the encapsulated functor and
    /// expects that result to be convertible to bool.  This functor
    /// is defined for up to three arguments.
    template <class T>
    struct logical_not
    {
        T t;
        explicit logical_not(T const & t=T()) : t(t) {}
    
        bool operator()() const { return !t(); }
    
        template <class A>
        bool operator()(A const & a) const { return !t(a); }

        template <class A, class B>
        bool operator()(A const & a, B const & b) const { return !t(a,b); }
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
