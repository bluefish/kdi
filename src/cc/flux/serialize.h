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

#ifndef FLUX_SERIALIZE_H
#define FLUX_SERIALIZE_H

#include <flux/stream.h>
#include <warp/file.h>
#include <assert.h>

namespace flux
{
    //------------------------------------------------------------------------
    // WriteFixed
    //------------------------------------------------------------------------
    struct WriteFixed
    {
        template <class T>
        void operator()(warp::FilePtr const & fp, T const & x)
        {
            assert(fp);
            fp->write(&x, sizeof(T), 1);
        }
    };


    //------------------------------------------------------------------------
    // Serialize
    //------------------------------------------------------------------------
    template <class T, class Writer = WriteFixed>
    class Serialize : public Stream<T>
    {
    public:
        typedef Serialize<T, Writer> my_t;
        typedef boost::shared_ptr<my_t> handle_t;

    private:
        warp::FilePtr fp;
        Writer write;

    public:
        explicit Serialize(warp::FilePtr const & fp,
                           Writer const & write = Writer()) :
            fp(fp), write(write)
        {
            assert(fp);
        }

        void put(T const & x)
        {
            write(fp, x);
        }

        void flush()
        {
            fp->flush();
        }
    };


    template <class T>
    typename Serialize<T>::handle_t makeSerialize(warp::FilePtr const & fp)
    {
        typename Serialize<T>::handle_t h(new Serialize<T>(fp));
        return h;
    }

    template <class T, class W>
    typename Serialize<T,W>::handle_t makeSerialize(warp::FilePtr const & fp,
                                                    W const & write)
    {
        typename Serialize<T,W>::handle_t h(new Serialize<T,W>(fp,write));
        return h;
    }
}

#endif // FLUX_SERIALIZE_H
