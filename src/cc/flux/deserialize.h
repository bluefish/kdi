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

#ifndef FLUX_DESERIALIZE_H
#define FLUX_DESERIALIZE_H

#include <flux/stream.h>
#include <warp/file.h>
#include <assert.h>

namespace flux
{
    //------------------------------------------------------------------------
    // ReadFixed
    //------------------------------------------------------------------------
    struct ReadFixed
    {
        template <class T>
        bool operator()(warp::FilePtr const & fp, T & x)
        {
            assert(fp);
            if(fp->read(&x, sizeof(T), 1))
                return true;
            else
                return false;
        }
    };


    //------------------------------------------------------------------------
    // Deserialize
    //------------------------------------------------------------------------
    template <class T, class Reader = ReadFixed>
    class Deserialize : public Stream<T>
    {
    public:
        typedef Deserialize<T, Reader> my_t;
        typedef boost::shared_ptr<my_t> handle_t;

    private:
        warp::FilePtr fp;
        Reader read;

    public:
        explicit Deserialize(warp::FilePtr const & fp,
                             Reader const & read = Reader()) :
            fp(fp), read(read)
        {
            assert(fp);
        }

        bool get(T & x)
        {
            return read(fp, x);
        }
    };


    template <class T>
    typename Deserialize<T>::handle_t makeDeserialize(warp::FilePtr const & fp)
    {
        typename Deserialize<T>::handle_t h(new Deserialize<T>(fp));
        return h;
    }

    template <class T, class R>
    typename Deserialize<T,R>::handle_t makeDeserialize(warp::FilePtr const & fp,
                                                        R const & read)
    {
        typename Deserialize<T,R>::handle_t h(new Deserialize<T,R>(fp,read));
        return h;
    }
}

#endif // FLUX_DESERIALIZE_H
