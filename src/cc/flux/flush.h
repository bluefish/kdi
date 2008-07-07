//---------------------------------------------------------- -*- Mode: C++ -*-
// Copyright (C) 2006 Josh Taylor (Kosmix Corporation)
// Created 2006-04-03
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

#ifndef FLUX_FLUSH_H
#define FLUX_FLUSH_H

#include <flux/stream.h>
#include <warp/util.h>
#include <assert.h>

namespace flux
{
    //------------------------------------------------------------------------
    // Flush
    //------------------------------------------------------------------------
    template <class T, class Sz=warp::size_of>
    class Flush : public Stream<T>
    {
    public:
        typedef Flush<T, Sz> my_t;
        typedef boost::shared_ptr<my_t> handle_t;

        typedef typename my_t::base_handle_t base_handle_t;

    private:
        base_handle_t output;
        size_t flushSize;
        size_t curSize;
        Sz sz;

    public:
        explicit Flush(size_t flushSize, Sz const & sz = Sz()) :
            flushSize(flushSize), curSize(0), sz(sz)
        {
        }

        void pipeTo(base_handle_t const & output)
        {
            this->output = output;
        }

        void put(T const & x)
        {
            assert(output);
            output->put(x);
            curSize += sz(x);
            if(curSize > flushSize)
                flush();
        }

        void flush()
        {
            assert(output);
            output->flush();
            curSize = 0;
        }
    };


    template <class T>
    typename Flush<T>::handle_t makeFlush(size_t flushSz)
    {
        typename Flush<T>::handle_t h(new Flush<T>(flushSz));
        return h;
    }

    template <class T, class Sz>
    typename Flush<T, Sz>::handle_t makeFlush(size_t flushSz, Sz const & sz)
    {
        typename Flush<T,Sz>::handle_t h(new Flush<T,Sz>(flushSz, sz));
        return h;
    }
}

#endif // FLUX_FLUSH_H
