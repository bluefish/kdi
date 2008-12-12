//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2006 Josh Taylor (Kosmix Corporation)
// Created 2008-12-11
// 
// This file is part of the flux library.
// 
// The flux library is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by the
// Free Software Foundation; either version 2 of the License, or any later
// version.
// 
// The flux library is distributed in the hope that it will be useful, but
// WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General
// Public License for more details.
// 
// You should have received a copy of the GNU General Public License along
// with this program; if not, write to the Free Software Foundation, Inc.,
// 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.
//----------------------------------------------------------------------------

#ifndef FLUX_CUTOFF_H
#define FLUX_CUTOFF_H

#include <flux/stream.h>
#include <warp/functional.h>
#include <boost/shared_ptr.hpp>

namespace flux
{
    //------------------------------------------------------------------------
    // Cutoff
    // ------------------------------------------------------------------------
    // Breaks an ordered stream into sections based on a cutoff value.
    // Items will be returned as long as they are less than the given
    // cutoff value.  Once the cutoff is reached, no more items are
    // returned until a new cutoff value is set.
    //
    template <class T, class V=T, class Lt=warp::less>
    class Cutoff: public Stream<T>
    {
    public:
        typedef Cutoff<T,V,Lt> my_t;
        typedef boost::shared_ptr<my_t> handle_t;
        typedef typename my_t::base_handle_t base_handle_t;

    private:
        base_handle_t input;
        bool buffered;
        bool cutoffSet;
        V cutoff;
        T current;
        Lt lt;

    public:
        explicit Cutoff(Lt const & lt = Lt()) :
            buffered(false),
            cutoffSet(false),
            lt(lt)
        {
        }

        void pipeFrom(base_handle_t const & input)
        {
            this->input = input;
        }

        bool get(T & x) {
            if(!buffered) {
                if(!input->get(current)) {
                    return false;
                } else {
                    buffered = true;
                }
            }

            if(!cutoffSet || lt(current, cutoff)) {
                x = current;
                buffered = false;
                return true;
            }

            return false;
        }

        my_t & setCutoff(const V & cutoff) {
            this->cutoff = cutoff;
            cutoffSet = true;
            return *this;
        }

        my_t & clearCutoff() {
            cutoffSet = false;
            return *this;
        }
    };

    template <class T>
    typename Cutoff<T>::handle_t
    makeCutoff()
    {
        typename Cutoff<T>::handle_t s(new Cutoff<T>());
        return s;
    }

    template <class T, class V>
    typename Cutoff<T,V>::handle_t
    makeCutoff()
    {
        typename Cutoff<T,V>::handle_t s(new Cutoff<T,V>());
        return s;
    }

    template <class T, class Lt>
    typename Cutoff<T,T,Lt>::handle_t
    makeCutoff(Lt const & lt)
    {
        typename Cutoff<T,T,Lt>::handle_t s(new Cutoff<T,T,Lt>(lt));
        return s;
    }

    template <class T, class V, class Lt>
    typename Cutoff<T,V,Lt>::handle_t
    makeCutoff(Lt const & lt)
    {
        typename Cutoff<T,V,Lt>::handle_t s(new Cutoff<T,V,Lt>(lt));
        return s;
    }
}

#endif // FLUX_JOIN_H
