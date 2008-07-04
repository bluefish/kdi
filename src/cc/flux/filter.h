//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2008 Josh Taylor (Kosmix Corporation)
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
#ifndef FLUX_FILTER_H
#define FLUX_FILTER_H

#include <flux/stream.h>

namespace flux {

    /// A Stream that uses a predicate functor to filter items passing
    /// through the stream.
    /// @param T The element type of the stream.
    /// @param P Predicate functor for filter.  It should return true
    /// for stream elements that should be kept, and false for
    /// elements to be discarded.
    template <class T, class P>
    class Filter;

}

//----------------------------------------------------------------------------
// Filter
//----------------------------------------------------------------------------
template <class T, class P>
class flux::Filter : public flux::Stream<T>
{
public:
    typedef Filter<T,P> my_t;
    typedef boost::shared_ptr<my_t> handle_t;
    typedef typename my_t::base_handle_t base_handle_t;

private:
    base_handle_t input;
    base_handle_t output;
    P pred;

public:
    explicit Filter(P const & pred) :
        pred(pred)
    {
    }

    void pipeFrom(base_handle_t const & input)
    {
        this->input = input;
    }

    void pipeTo(base_handle_t const & output)
    {
        this->output = output;
    }

    bool get(T & x)
    {
        if(!input)
            return false;

        while(input->get(x))
            if(pred(x))
                return true;

        return false;
    }

    void put(T const & x)
    {
        if(output && pred(x))
            output->put(x);
    }

    void flush()
    {
        if(output)
            output->flush();
    }
    
    bool fetch()
    {
        return input && input->fetch();
    }
};

namespace flux
{
    template <class T, class P>
    typename Filter<T,P>::handle_t
    makeFilter(P const & pred)
    {
        typename Filter<T,P>::handle_t s(new Filter<T,P>(pred));
        return s;
    }
}

#endif // FLUX_FILTER_H
