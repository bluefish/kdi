//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/TASKS/josh.crawler/src/cc/flux/deserialize.h#1 $
//
// Created 2006/04/03
//
// Copyright 2006 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// 
//----------------------------------------------------------------------------

#ifndef FLUX_DESERIALIZE_H
#define FLUX_DESERIALIZE_H

#include "stream.h"
#include "warp/file.h"
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
