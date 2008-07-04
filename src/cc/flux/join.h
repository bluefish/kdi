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

#ifndef FLUX_JOIN_H
#define FLUX_JOIN_H

#include <flux/composite.h>
#include <vector>
#include <functional>
#include <boost/shared_ptr.hpp>

namespace flux
{
    //------------------------------------------------------------------------
    // Join
    //------------------------------------------------------------------------
    /// Ties multiple Streams together and offers an equal-value
    /// segmented view on each, where equality is inferred from the
    /// user-supplied ordering function.
    template <class T, class Lt=std::less<T> >
    class Join : public Composite<T>
    {
    public:
        typedef Join<T, Lt> my_t;
        typedef boost::shared_ptr<my_t> handle_t;

        typedef typename my_t::base_handle_t base_handle_t;

    private:
        struct State
        {
            base_handle_t input;
            T value;
            bool valid;

            State() : valid(false) {}
            State(base_handle_t const & i) : input(i), valid(false) {}

            void takeFrom(T & x)
            {
                value = x;
                x = T();
                valid = true;
            }

            void releaseTo(T & x)
            {
                x = value;
                value = T();
                valid = false;
            }
        };

        typedef std::vector<State> state_vec_t;

        state_vec_t state;
        T minValue;
        bool minValid;
        Lt lt;

    public:
        explicit Join(Lt const & lt=Lt()) :
            minValid(false), lt(lt) {}

        void pipeFrom(base_handle_t const & input)
        {
            // Add an input channel
            assert(input);
            state.push_back(input);

            // Reset any segment we might currently be in
            if(minValid)
            {
                minValue = T();
                minValid = false;
            }
        }

        bool get(int idx, T & x)
        {
            assert(idx >= 0 && (size_t)idx < state.size());

            // Only return items if the segment has been initialized
            // with fetch()
            if(!minValid)
                return false;

            // Get the stream we're working with
            State & s = state[idx];
            
            // Check to see if we're holding a value
            if(s.valid)
            {
                // If the min is less than the held value, it belongs
                // in the next segment
                if(lt(minValue, s.value))
                    return false;

                // Otherwise, return the held value
                s.releaseTo(x);
                return true;
            }

            // Try to get another item from the input stream
            if(!s.input->get(x))
                return false;

            // If the min is less than the new item, hold it for a
            // later segment
            if(lt(minValue, x))
            {
                s.takeFrom(x);
                return false;
            }

            // Otherwise, return the new item
            return true;
        }

        bool fetch()
        {
            // If we were in a segment, advance all channels to the
            // end of the segment
            if(minValid)
            {
                T tmp;
                int nInputs = state.size();
                for(int idx = 0; idx < nInputs; ++idx)
                {
                    while(get(idx, tmp))
                        ;
                }
            }

            // Find the min value for the next segment
            minValid = false;
            for(typename state_vec_t::iterator si = state.begin();
                si != state.end(); ++si)
            {
                // Check if this stream has a held value, or we can
                // get one
                if(si->valid || si->input->get(si->value))
                {
                    // This stream now has a held value
                    si->valid = true;

                    // Check to see if it is the new min
                    if(!minValid || lt(si->value, minValue))
                    {
                        minValue = si->value;
                        minValid = true;
                    }
                }
            }

            // Return true if we found a min value
            if(minValid)
                return true;
            else
            {
                minValue = T();
                return false;
            }
        }
    };

    template <class T>
    typename Join<T>::handle_t makeJoin()
    {
        typename Join<T>::handle_t s(new Join<T>());
        return s;
    }

    template <class T, class Lt>
    typename Join<T,Lt>::handle_t makeJoin(Lt const & lt)
    {
        typename Join<T,Lt>::handle_t s(new Join<T,Lt>(lt));
        return s;
    }
}

#endif // FLUX_JOIN_H
