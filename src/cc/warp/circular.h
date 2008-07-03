//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
// Created 2008-06-10
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

#ifndef WARP_CIRCULAR_H
#define WARP_CIRCULAR_H

#include <boost/noncopyable.hpp>
#include <boost/utility/enable_if.hpp>
#include <boost/iterator/iterator_facade.hpp>

namespace warp {

    template <class T>
    class Circular;

    template <class T>
    class CircularList;

} // namespace warp

//----------------------------------------------------------------------------
// Circular
//----------------------------------------------------------------------------
template <class T>
class warp::Circular
    : private boost::noncopyable
{
    typedef Circular<T> my_t;

    my_t * p;
    my_t * n;

    /// Remove this node from any list it may be in.  This node isn't
    /// modified, but the adjacent nodes are made to point to each
    /// other.
    void removeFromList()
    {
        p->n = n;
        n->p = p;
    }

protected:
    Circular() : p(this), n(this) {}

public:
    ~Circular() { removeFromList(); }

    my_t * prev() { return p; }
    my_t const * prev() const { return p; }

    my_t * next() { return n; }
    my_t const * next() const { return n; }

    T * cast() { return static_cast<T *>(this); }
    T const * cast() const { return static_cast<T const *>(this); }

    /// Return true if this node is the only element in its list.
    bool isSingular() const { return n == this; }

    /// Remove this node from any list it may be in.
    void unlink()
    {
        removeFromList();
        p = n = this;
    }

    /// Take X out of whatever list it may be in and make it the
    /// successor of this item.
    void setSuccessor(my_t * x)
    {
        x->removeFromList();
        x->p = this;
        x->n = n;
        n->p = x;
        n = x;
    }

    /// Take X out of whatever list it may be in and make it the
    /// predecessor of this item.
    void setPredecessor(my_t * x)
    {
        x->removeFromList();
        x->n = this;
        x->p = p;
        p->n = x;
        p = x;
    }
};

//----------------------------------------------------------------------------
// CircularList
//----------------------------------------------------------------------------
template <class T>
class warp::CircularList
    : private boost::noncopyable
{
    struct Pivot : public Circular<T> {};
    Pivot pivot;

private:
    template <class V, class P>
    class iter :
        public boost::iterator_facade<iter<V,P>, V, boost::bidirectional_traversal_tag>
    {
        struct enabler {};
        P p;

    public:
        iter() : p(0) {}
        explicit iter(P p) : p(p) {}

        template <class V2,class P2>
        iter(
            iter<V2,P2> const & o,
            typename boost::enable_if<boost::is_convertible<P2,P>, enabler>::type = enabler()
            ) : p(o.base()) {}

        P base() const { return p; }
        
    private:
        friend class boost::iterator_core_access;

        template <class V2,class P2>
        bool equal(iter<V2,P2> const & o) const { return p == o.base(); }

        typename iter::reference dereference() const { return *p->cast(); }
        void increment() { p = p->next(); }
        void decrement() { p = p->prev(); }
    };

public:
    typedef iter<T, Circular<T> *> iterator;
    typedef iter<T const, Circular<T> const *> const_iterator;

public:
    iterator begin() { return ++iterator(&pivot); }
    iterator end() { return iterator(&pivot); }

    const_iterator begin() const { return ++const_iterator(&pivot); }
    const_iterator end() const { return const_iterator(&pivot); }

    void moveToFront(T * x) { pivot.setSuccessor(x); }
    void moveToBack(T * x) { pivot.setPredecessor(x); }

    T & front() { return *pivot.next()->cast(); }
    T const & front() const { return *pivot.next()->cast(); }

    T & back() { return *pivot.prev()->cast(); }
    T const & back() const { return *pivot.prev()->cast(); }

    bool empty() const { return pivot.isSingular(); }
};


#endif // WARP_CIRCULAR_H
