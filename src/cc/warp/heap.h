//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
// Created 2007-01-31
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

#ifndef WARP_HEAP_H
#define WARP_HEAP_H

#include <algorithm>
#include <vector>
#include "functional.h"

namespace warp
{
    /// A heap that keeps the max element on top.  Order is defined by the
    /// given less-than functor.
    /// @param T Type of element in heap
    /// @param Lt Binary predicate functor that indicates if its first
    /// argument is less than its second argument.
    template <class T, class Lt=std::less<T> >
    class MaxHeap;

    /// A heap that keeps the min element on top.  Order is defined by the
    /// given less-than functor.
    /// @param T Type of element in heap
    /// @param Lt Binary predicate functor that indicates if its first
    /// argument is less than its second argument.
    template <class T, class Lt=std::less<T> >
    class MinHeap;
}

//----------------------------------------------------------------------------
// MaxHeap
//----------------------------------------------------------------------------
template <class T, class Lt>
class warp::MaxHeap
{
public:
    typedef T value_t;

private:
    typedef MaxHeap<T, Lt> my_t;
    typedef std::vector<T> vec_t;

    vec_t v;
    Lt lt;

public:
    /// Construct an empty heap.
    explicit MaxHeap(Lt const & lt = Lt()) : lt(lt) {}

    /// Construct a heap from an iterator range of items.
    template <class It>
    MaxHeap(It first, It last, Lt const & lt = Lt()) :
        v(first, last), lt(lt)
    {
        std::make_heap(first, last, lt);
    }

    /// Pre-allocate space in heap for a certain number of items.
    /// This is not necessary for correct operation, but it can
    /// improve performance by reducing the number of resizing
    /// allocations.
    void reserve(size_t nItems)
    {
        v.reserve(nItems);
    }

    /// Add a new item to the heap.
    void push(value_t const & x)
    {
        v.push_back(x);
        std::push_heap(v.begin(), v.end(), lt);
    }

    /// Get top item in heap.  The item is not removed from the heap.
    /// The top item has the maximal value according to the given
    /// ordering function.
    value_t const & top() const
    {
        return v.front();
    }

    /// Remove top item from the heap.
    void pop()
    {
        std::pop_heap(v.begin(), v.end(), lt);
        v.pop_back();
    }

    /// Return true iff the given item is under the current top of the
    /// heap.  For a fixed-size heap, this can be used to decide
    /// whether to push a new item into the heap.  If the new item is
    /// over the current top, it would be the first item popped and so
    /// need not be inserted at all.
    bool underTop(value_t const & item) const
    {
        return lt(item, top());
    }

    /// Remove the top item from the heap and push the given value
    /// into the heap.  This is equivalent to a call to pop() followed
    /// by a call to push(), though it is potentially more efficient.
    /// Note that the old top item will be removed even if the new
    /// item is over it in the heap order.  If this is not the desired
    /// behavior, underTop() can be used to decide whether replace()
    /// should be called.  A typical insertion into a bounded heap
    /// looks like:
    ///
    ///   if(heap.size() < MAX_HEAP_SIZE)
    ///       heap.push(item);
    ///   else if(heap.underTop(item))
    ///       heap.replace(item);
    void replace(value_t const & v)
    {
        pop();
        push(v);
    }

    /// Remove all items from heap.
    void clear()
    {
        v.clear();
    }

    /// Return true if heap is empty.
    bool empty() const
    {
        return v.empty();
    }

    /// Get number of items in heap.
    size_t size() const
    {
        return v.size();
    }
};


//----------------------------------------------------------------------------
// MinHeap
//----------------------------------------------------------------------------
template <class T, class Lt>
class warp::MinHeap : public warp::MaxHeap<T, warp::reverse_order<T,Lt> >
{
private:
    typedef MinHeap<T, Lt> my_t;
    typedef MaxHeap<T, reverse_order<T,Lt> > super;

public:
    /// Construct an empty heap.
    explicit MinHeap(Lt const & lt = Lt()) : super(reverse_order<T,Lt>(lt)) {}

    /// Construct a heap from an iterator range of items.
    template <class It>
    MinHeap(It first, It last, Lt const & lt = Lt()) :
        super(first, last, reverse_order<T,Lt>(lt)) {}
};


#endif // WARP_HEAP_H
