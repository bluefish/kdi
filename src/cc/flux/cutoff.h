//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2006 Josh Taylor (Kosmix Corporation)
// Created 2006-01-11
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
#include <vector>
#include <functional>
#include <boost/shared_ptr.hpp>

namespace flux
{
    //------------------------------------------------------------------------
    // Cutoff 
    //------------------------------------------------------------------------
    // Breaks a stream into sections based on a cutoff value.
    // Once the cutoff is reached, no more items are returned until a new
    // cutoff value is set.
    //
    template <class T, class Lt=std::less<T> >
    class Cutoff: public Stream<T>
    {
    public:
        typedef Cutoff<T, Lt> my_t;
        typedef boost::shared_ptr<my_t> handle_t;
        typedef typename my_t::base_handle_t base_handle_t;

    private:
        base_handle_t input;
        bool buffered;
        bool cutoffSet;
        T cutoff;
        T current;

    public:
        explicit Cutoff() :
            buffered(false),
            cutoffSet(false)
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

            if(!cutoffSet || current < cutoff) {
                x = current;
                buffered = false;
                return true;
            }

            return false;
        }

        my_t & setCutoff(const T & cutoff) {
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
}

#endif // FLUX_JOIN_H
