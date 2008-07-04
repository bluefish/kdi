//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2006 Josh Taylor (Kosmix Corporation)
// Created 2006-01-11
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

#ifndef FLUX_MERGE_H
#define FLUX_MERGE_H

#include <flux/stream.h>
#include <warp/heap.h>
#include <list>
#include <functional>

namespace flux {

    /// Ordered merge of multiple input streams.  Each get() returns
    /// the minimum next item from input set.  In case of value ties,
    /// the inputs are ranked by the order they were added to the
    /// merge.  Merged items will come from the earliest input first.
    template <class T, class Lt=std::less<T> >
    class Merge;

}

//----------------------------------------------------------------------------
// Merge
//----------------------------------------------------------------------------
template <class T, class Lt>
class flux::Merge : public flux::Stream<T>
{
public:
    typedef Merge<T,Lt> my_t;
    typedef boost::shared_ptr<my_t> handle_t;

    typedef typename my_t::base_handle_t base_handle_t;

    typedef T value_t;
    typedef Lt value_cmp_t;

private:
    /// State for each input stream
    struct Input
    {
        base_handle_t stream;
        size_t index;
        value_t value;
        bool hasValue;

        Input(base_handle_t const & stream, size_t index) :
            stream(stream),
            index(index),
            hasValue(false)
        {
        }

        /// Read the next value out of the input stream.  Return true
        /// iff there is such a value.
        bool readNext()
        {
            hasValue = stream->get(value);
            if(!hasValue)
                value = value_t();
            return hasValue;
        }
    };

    /// Ordering function for choosing the next input in the merge
    struct InputOrder
    {
        value_cmp_t valueLt;
        
        InputOrder(value_cmp_t const & valueLt) : valueLt(valueLt) {}

        bool operator()(Input const * a, Input const * b) const
        {
            // Order first by value, then by stream index
            if(valueLt(a->value, b->value))
                return true;
            else if(valueLt(b->value, a->value))
                return false;
            else
                return a->index < b->index;
        }
    };

    typedef std::list<Input> list_t;
    typedef warp::MinHeap<Input *, InputOrder> heap_t;

    list_t inputs;
    heap_t minHeap;
    bool inputChanged;
    bool mergeUnique;
    value_cmp_t valueLt;

    /// Advance the top input on the heap and update the heap.  If the
    /// top input is exhausted, it is removed from the heap.
    /// Otherwise, it is replaced in the heap with its new value,
    /// allowing another input to move to the top.  The heap must not
    /// be empty before calling this function.
    void advance()
    {
        assert(!minHeap.empty());

        // Get the top input
        Input * top = minHeap.top();

        // Get next value in top input stream
        if(top->readNext())
        {
            // Got one, put input back in the heap
            minHeap.replace(top);
        }
        else
        {
            // Input is exhausted, remove from heap
            minHeap.pop();
        }
    }
    
public:
    Merge() :
        minHeap(InputOrder(Lt())),
        inputChanged(false),
        mergeUnique(false),
        valueLt(Lt())
    {}

    explicit Merge(Lt const & lt) :
        minHeap(InputOrder(lt)),
        inputChanged(false),
        mergeUnique(false),
        valueLt(lt)
    {}

    Merge(bool mergeUnique, Lt const & lt) :
        minHeap(InputOrder(lt)),
        inputChanged(false),
        mergeUnique(mergeUnique),
        valueLt(lt)
    {}

    void pipeFrom(base_handle_t const & input)
    {
        using namespace ex;
        if(!input)
            raise<ValueError>("null input");

        inputs.push_back(Input(input, inputs.size()));
        inputChanged = true;
    }

    bool get(T & x)
    {
        if(inputChanged)
        {
            // Input set has changed, fetch it into the heap
            fetch();
            inputChanged = false;
        }

        // If there's nothing left in the heap, we're done
        if(minHeap.empty())
            return false;

        // Grab the top value and advance the input
        x = minHeap.top()->value;
        advance();

        // If we're doing a unique merge, consume all duplicates
        if(mergeUnique)
        {
            // Advance our inputs until we get to a value greater than
            // the current return value or we run out of inputs
            while(!minHeap.empty() && !valueLt(x, minHeap.top()->value))
                advance();
        }

        // Done -- we got the top item
        return true;
    }

    bool fetch()
    {
        // Add items in the input set which have new data
        for(typename list_t::iterator it = inputs.begin();
            it != inputs.end(); ++it)
        {
            // Don't add inputs already in the heap
            if(it->hasValue)
                continue;
                
            // Don't add inputs that have no data
            if(!it->readNext())
                continue;

            // Add this input to the heap
            minHeap.push(&*it);
        }

        // Return true if there's something left to merge
        return !minHeap.empty();
    }
};

namespace flux {

    /// Helper function to create Merge stream.
    template <class T>
    typename Merge<T>::handle_t makeMerge()
    {
        typename Merge<T>::handle_t s(new Merge<T>());
        return s;
    }

    /// Helper function to create Merge stream.
    template <class T>
    typename Merge<T>::handle_t makeMergeUnique()
    {
        typename Merge<T>::handle_t s(
            new Merge<T>(
                true,
                typename Merge<T>::value_cmp_t()
                )
            );
        return s;
    }

    /// Helper function to create Merge stream.
    template <class T, class Lt>
    typename Merge<T,Lt>::handle_t makeMerge(Lt const & lt)
    {
        typename Merge<T,Lt>::handle_t s(new Merge<T,Lt>(lt));
        return s;
    }

    /// Helper function to create Merge stream.
    template <class T, class Lt>
    typename Merge<T,Lt>::handle_t makeMergeUnique(Lt const & lt)
    {
        typename Merge<T,Lt>::handle_t s(new Merge<T,Lt>(true, lt));
        return s;
    }

    /// Helper function to create Merge stream.
    template <class T, class Lt>
    typename Merge<T,Lt>::handle_t makeMerge(bool mergeUnique, Lt const & lt)
    {
        typename Merge<T,Lt>::handle_t s(new Merge<T,Lt>(mergeUnique, lt));
        return s;
    }

}

#endif // FLUX_MERGE_H
