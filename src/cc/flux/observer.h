//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2006 Josh Taylor (Kosmix Corporation)
// Created 2006-03-03
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

#ifndef FLUX_OBSERVER_H
#define FLUX_OBSERVER_H

#include <flux/stream.h>
#include <assert.h>

namespace flux
{
    //------------------------------------------------------------------------
    // Observer
    //------------------------------------------------------------------------
    template <class T, class Cb>
    class Observer : public Stream<T>
    {
    public:
        typedef Observer<T,Cb> my_t;
        typedef boost::shared_ptr<my_t> handle_t;

        typedef typename my_t::base_handle_t base_handle_t;

    private:
        base_handle_t stream;
        Cb cb;

    public:
        explicit Observer(Cb const & cb = Cb()) :
            cb(cb)
        {}

        void pipeFrom(base_handle_t const & input)
        {
            stream = input;
        }

        void pipeTo(base_handle_t const & output)
        {
            stream = output;
        }

        bool get(T & x)
        {
            assert(stream);
            if(stream->get(x))
            {
                cb(*this, x);
                return true;
            }
            else
                return false;
        }

        void put(T const & x)
        {
            cb(*this, x);
            if(stream)
                stream->put(x);
        }

        bool fetch()
        {
            if(stream)
                return stream->fetch();
            else
                return false;
        }

        void flush()
        {
            if(stream)
                stream->flush();
        }
    };

    template <class T, class Cb>
    typename Observer<T,Cb>::handle_t makeObserver(Cb const & cb)
    {
        typename Observer<T,Cb>::handle_t s(new Observer<T,Cb>(cb));
        return s;
    }
}

#endif // FLUX_OBSERVER_H
